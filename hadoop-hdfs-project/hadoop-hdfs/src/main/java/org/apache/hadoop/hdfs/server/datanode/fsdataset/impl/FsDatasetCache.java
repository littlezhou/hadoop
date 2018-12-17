/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.Pmem;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.PmemMappedRegion;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages caching for an FsDatasetImpl by using the mmap(2) and mlock(2)
 * system calls to lock blocks into memory. Block checksums are verified upon
 * entry into the cache.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsDatasetCache {
  /**
   * MappableBlocks that we know about.
   */
  private static final class Value {
    final State state;
    final MappableBlock mappableBlock;

    Value(MappableBlock mappableBlock, State state) {
      this.mappableBlock = mappableBlock;
      this.state = state;
    }
  }

  private enum State {
    /**
     * The MappableBlock is in the process of being cached.
     */
    CACHING,

    /**
     * The MappableBlock was in the process of being cached, but it was
     * cancelled.  Only the FsDatasetCache#WorkerTask can remove cancelled
     * MappableBlock objects.
     */
    CACHING_CANCELLED,

    /**
     * The MappableBlock is in the cache.
     */
    CACHED,

    /**
     * The MappableBlock is in the process of uncaching.
     */
    UNCACHING;

    /**
     * Whether we should advertise this block as cached to the NameNode and
     * clients.
     */
    public boolean shouldAdvertise() {
      return (this == CACHED);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FsDatasetCache
      .class);

  /**
   * Stores MappableBlock objects and the states they're in.
   */
  private final HashMap<ExtendedBlockId, Value> mappableBlockMap =
      new HashMap<ExtendedBlockId, Value>();

  private final AtomicLong numBlocksCached = new AtomicLong(0);

  private final FsDatasetImpl dataset;

  private final ThreadPoolExecutor uncachingExecutor;

  private final ScheduledThreadPoolExecutor deferredUncachingExecutor;

  private final long revocationMs;

  private final long revocationPollingMs;

  /**
   * The approximate amount of cache space in use.
   *
   * This number is an overestimate, counting bytes that will be used only
   * if pending caching operations succeed.  It does not take into account
   * pending uncaching operations.
   *
   * This overestimate is more useful to the NameNode than an underestimate,
   * since we don't want the NameNode to assign us more replicas than
   * we can cache, because of the current batch of operations.
   */
  private final UsedBytesCount usedBytesCount;

  public static class PageRounder {
    private final long osPageSize =
        NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();

    /**
     * Round up a number to the operating system page size.
     */
    public long roundUp(long count) {
      return (count + osPageSize - 1) & (~(osPageSize - 1));
    }

    /**
     * Round down a number to the operating system page size.
     */
    public long roundDown(long count) {
      return count & (~(osPageSize - 1));
    }
  }

  private class UsedBytesCount {
    private final AtomicLong usedBytes = new AtomicLong(0);
    
    private final PageRounder rounder = new PageRounder();

    /**
     * Try to reserve more bytes.
     *
     * @param count    The number of bytes to add.  We will round this
     *                 up to the page size.
     *
     * @return         The new number of usedBytes if we succeeded;
     *                 -1 if we failed.
     */
    long reserve(long count) {
      count = rounder.roundUp(count);
      while (true) {
        long cur = usedBytes.get();
        long next = cur + count;
        if (next > maxBytes) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }
    
    /**
     * Release some bytes that we're using.
     *
     * @param count    The number of bytes to release.  We will round this
     *                 up to the page size.
     *
     * @return         The new number of usedBytes.
     */
    long release(long count) {
      count = rounder.roundUp(count);
      return usedBytes.addAndGet(-count);
    }

    /**
     * Release some bytes that we're using rounded down to the page size.
     *
     * @param count    The number of bytes to release.  We will round this
     *                 down to the page size.
     *
     * @return         The new number of usedBytes.
     */
    long releaseRoundDown(long count) {
      count = rounder.roundDown(count);
      return usedBytes.addAndGet(-count);
    }

    long get() {
      return usedBytes.get();
    }
  }

  /**
   * Manage the non-volatile storage class memory cache volumes.
   *
   * // TODO: Refine persistent location considering storage utilization
   */
  public static class PmemVolumeManager {
    private final ArrayList<String> pmemVolumes = new ArrayList<String>();
    // It's not a strict atomic operation for the performance sake.
    private int index = 0;
    private int count = 0;

    public void load(String[] volumes) throws IOException {
      // Check does the directory exist
      for (String location: volumes) {
        try {
          File locFile = new File(location);
          verifyIfValidPmemVolume(locFile);
          // Remove all files under the volume. Files may been left after a
          // unexpected data node restart.
          FileUtils.cleanDirectory(locFile);
        } catch (IllegalArgumentException e) {
          LOG.error("Fail to parse persistent memory location " + location +
              " for " + e.getMessage());
          throw new IOException(e);
        }
        pmemVolumes.add(location);
        LOG.info("Added persistent memory - " + location);
      }
      count = pmemVolumes.size();
    }

    public static void verifyIfValidPmemVolume(File pmemDir)
        throws IOException {
      if (!pmemDir.exists()) {
        final String message = pmemDir + " does not exist";
        throw new IOException(message);
      }

      if (!pmemDir.isDirectory()) {
        final String message = pmemDir + " is not a directory";
        throw new IllegalArgumentException(message);
      }

      String uuidStr = UUID.randomUUID().toString();
      String testFile = pmemDir.getPath() + "/.verify.pmem." + uuidStr;
      byte[] contents = uuidStr.getBytes("UTF-8");
      PmemMappedRegion region = null;
      try {
        region = Pmem.mapBlock(testFile, contents.length);
        if (region == null) {
          throw new IOException("Fail to map into persistent storage.");
        }
        Pmem.memCopy(contents, region.getAddress(), region.isPmem(),
            contents.length);
        Pmem.memSync(region);
      } catch (Throwable t) {
        throw new IOException(t);
      } finally {
        if (region != null) {
          Pmem.unmapBlock(region.getAddress(), region.getLength());
          boolean deled = false;
          String reason = null;
          try {
            deled = new File(testFile).delete();
          } catch (Throwable t) {
            reason = t.getMessage();
          }
          if (!deled) {
            LOG.warn("Failed to delete persistent memory test file " +
                testFile + (reason == null ? "" : " due to: " + reason));
          }
        }
      }
    }

    /**
     * Choose a persistent location based on specific algorithms.
     * Currently it is a round-robin policy.
     */
    public String getOneLocation() {
      if (count != 0) {
        return pmemVolumes.get(index++ % count);
      } else {
        throw new RuntimeException("No usable persistent memory are found");
      }
    }
  }
  /**
   * The total cache capacity in bytes.
   */
  private final long maxBytes;

  private PmemVolumeManager pmemManager;

  /**
   * Number of cache commands that could not be completed successfully
   */
  final AtomicLong numBlocksFailedToCache = new AtomicLong(0);
  /**
   * Number of uncache commands that could not be completed successfully
   */
  final AtomicLong numBlocksFailedToUncache = new AtomicLong(0);

  public FsDatasetCache(FsDatasetImpl dataset) throws IOException {
    this.dataset = dataset;
    this.maxBytes = dataset.datanode.getDnConf().getMaxLockedMemory();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsDatasetCache-%d-" + dataset.toString())
        .build();
    this.usedBytesCount = new UsedBytesCount();
    this.uncachingExecutor = new ThreadPoolExecutor(
            0, 1,
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            workerFactory);
    this.uncachingExecutor.allowCoreThreadTimeOut(true);
    this.deferredUncachingExecutor = new ScheduledThreadPoolExecutor(
            1, workerFactory);
    this.revocationMs = dataset.datanode.getConf().getLong(
        DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS,
        DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT);
    long confRevocationPollingMs = dataset.datanode.getConf().getLong(
        DFS_DATANODE_CACHE_REVOCATION_POLLING_MS,
        DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT);
    long minRevocationPollingMs = revocationMs / 2;
    if (minRevocationPollingMs < confRevocationPollingMs) {
      throw new RuntimeException("configured value " +
              confRevocationPollingMs + "for " +
              DFS_DATANODE_CACHE_REVOCATION_POLLING_MS +
              " is too high.  It must not be more than half of the " +
              "value of " +  DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS +
              ".  Reconfigure this to " + minRevocationPollingMs);
    }
    this.revocationPollingMs = confRevocationPollingMs;

    String[] pmemVolumes = dataset.datanode.getDnConf().getPmemVolumes();
    if (pmemVolumes != null && pmemVolumes.length != 0) {
      if (!NativeIO.isAvailable() || !NativeIO.POSIX.isPmemAvailable()) {
        throw new IOException("Persistent memory storage configured, " +
            "but not available (" + NativeIO.POSIX.PMDK_SUPPORT_STATE + ").");
      }
      this.pmemManager = new PmemVolumeManager();
      this.pmemManager.load(pmemVolumes);
      PmemMappedBlock.setPersistentMemoryManager(this.pmemManager);
      PmemMappedBlock.setDataset(dataset);
    }
  }

  /**
   * @return List of cached blocks suitable for translation into a
   * {@link BlockListAsLongs} for a cache report.
   */
  synchronized List<Long> getCachedBlocks(String bpid) {
    List<Long> blocks = new ArrayList<Long>();
    for (Iterator<Entry<ExtendedBlockId, Value>> iter =
        mappableBlockMap.entrySet().iterator(); iter.hasNext(); ) {
      Entry<ExtendedBlockId, Value> entry = iter.next();
      if (entry.getKey().getBlockPoolId().equals(bpid)) {
        if (entry.getValue().state.shouldAdvertise()) {
          blocks.add(entry.getKey().getBlockId());
        }
      }
    }
    return blocks;
  }

  /**
   * Attempt to begin caching a block.
   */
  synchronized void cacheBlock(long blockId, String bpid,
      String blockFileName, long length, long genstamp,
      Executor volumeExecutor) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);
    if (prevValue != null) {
      LOG.debug("Block with id {}, pool {} already exists in the "
              + "FsDatasetCache with state {}", blockId, bpid, prevValue.state
      );
      numBlocksFailedToCache.incrementAndGet();
      return;
    }
    mappableBlockMap.put(key, new Value(null, State.CACHING));
    volumeExecutor.execute(
        new CachingTask(key, blockFileName, length, genstamp));
    LOG.debug("Initiating caching for Block with id {}, pool {}", blockId,
        bpid);
  }

  synchronized void uncacheBlock(String bpid, long blockId) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);
    boolean deferred = false;

    if (!dataset.datanode.getShortCircuitRegistry().
            processBlockMunlockRequest(key)) {
      deferred = true;
    }
    if (prevValue == null) {
      LOG.debug("Block with id {}, pool {} does not need to be uncached, "
          + "because it is not currently in the mappableBlockMap.", blockId,
          bpid);
      numBlocksFailedToUncache.incrementAndGet();
      return;
    }
    switch (prevValue.state) {
    case CACHING:
      LOG.debug("Cancelling caching for block with id {}, pool {}.", blockId,
          bpid);
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.CACHING_CANCELLED));
      break;
    case CACHED:
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.UNCACHING));
      if (deferred) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} is anchored, and can't be uncached now.  Scheduling it " +
                  "for uncaching in {} ",
              key, DurationFormatUtils.formatDurationHMS(revocationPollingMs));
        }
        deferredUncachingExecutor.schedule(
            new UncachingTask(key, revocationMs),
            revocationPollingMs, TimeUnit.MILLISECONDS);
      } else {
        LOG.debug("{} has been scheduled for immediate uncaching.", key);
        uncachingExecutor.execute(new UncachingTask(key, 0));
      }
      break;
    default:
      LOG.debug("Block with id {}, pool {} does not need to be uncached, "
          + "because it is in state {}.", blockId, bpid, prevValue.state);
      numBlocksFailedToUncache.incrementAndGet();
      break;
    }
  }

  /**
   * Try to reserve more bytes.
   *
   * @param count    The number of bytes to add.  We will round this
   *                 up to the page size.
   *
   * @return         The new number of usedBytes if we succeeded;
   *                 -1 if we failed.
   */
  long reserve(long count) {
    return usedBytesCount.reserve(count);
  }

  /**
   * Release some bytes that we're using.
   *
   * @param count    The number of bytes to release.  We will round this
   *                 up to the page size.
   *
   * @return         The new number of usedBytes.
   */
  long release(long count) {
    return usedBytesCount.release(count);
  }

  /**
   * Release some bytes that we're using rounded down to the page size.
   *
   * @param count    The number of bytes to release.  We will round this
   *                 down to the page size.
   *
   * @return         The new number of usedBytes.
   */
  long releaseRoundDown(long count) {
    return usedBytesCount.releaseRoundDown(count);
  }

  /**
   * Get the OS page size.
   *
   * @return the OS page size.
   */
  long getOsPageSize() {
    return usedBytesCount.rounder.osPageSize;
  }

  /**
   * Round up to the OS page size.
   */
  long roundUpPageSize(long count) {
    return usedBytesCount.rounder.roundUp(count);
  }

  /**
   * Background worker that mmaps, mlocks, and checksums a block
   */
  private class CachingTask implements Runnable {
    private final ExtendedBlockId key; 
    private final String blockFileName;
    private final long length;
    private final long genstamp;

    CachingTask(ExtendedBlockId key, String blockFileName, long length,
        long genstamp) {
      this.key = key;
      this.blockFileName = blockFileName;
      this.length = length;
      this.genstamp = genstamp;
    }

    @Override
    public void run() {
      boolean success = false;
      FileInputStream blockIn = null, metaIn = null;
      MappableBlock mappableBlock = null;
      ExtendedBlock extBlk = new ExtendedBlock(key.getBlockPoolId(),
          key.getBlockId(), length, genstamp);
      long newUsedBytes = reserve(length);
      boolean reservedBytes = false;
      try {
        if (newUsedBytes < 0) {
          LOG.warn("Failed to cache " + key + ": could not reserve " + length +
              " more bytes in the cache: " +
              DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY +
              " of " + maxBytes + " exceeded.");
          return;
        }
        reservedBytes = true;
        try {
          blockIn = (FileInputStream)dataset.getBlockInputStream(extBlk, 0);
          metaIn = DatanodeUtil.getMetaDataInputStream(extBlk, dataset);
        } catch (ClassCastException e) {
          LOG.warn("Failed to cache " + key +
              ": Underlying blocks are not backed by files.", e);
          return;
        } catch (FileNotFoundException e) {
          LOG.info("Failed to cache " + key + ": failed to find backing " +
              "files.");
          return;
        } catch (IOException e) {
          LOG.warn("Failed to cache " + key + ": failed to open file", e);
          return;
        }
        try {
          // Currently user can only choose either memory or persistent memory
          // to cache the data.
          if (pmemManager == null) {
            mappableBlock = MemoryMappedBlock.load(length, blockIn, metaIn,
                blockFileName, key);
          } else {
            mappableBlock = PmemMappedBlock.load(length, blockIn, metaIn,
                blockFileName, key);
          }
        } catch (ChecksumException e) {
          // Exception message is bogus since this wasn't caused by a file read
          LOG.warn("Failed to cache " + key + ": checksum verification failed.");
          return;
        } catch (IOException e) {
          LOG.warn("Failed to cache " + key, e);
          return;
        }
        synchronized (FsDatasetCache.this) {
          Value value = mappableBlockMap.get(key);
          Preconditions.checkNotNull(value);
          Preconditions.checkState(value.state == State.CACHING ||
                                   value.state == State.CACHING_CANCELLED);
          if (value.state == State.CACHING_CANCELLED) {
            mappableBlockMap.remove(key);
            LOG.warn("Caching of " + key + " was cancelled.");
            return;
          }
          mappableBlockMap.put(key, new Value(mappableBlock, State.CACHED));
          mappableBlock.afterCache();
        }
        LOG.debug("Successfully cached {}.  We are now caching {} bytes in"
            + " total.", key, newUsedBytes);
        dataset.datanode.getShortCircuitRegistry().processBlockMlockEvent(key);
        numBlocksCached.addAndGet(1);
        dataset.datanode.getMetrics().incrBlocksCached(1);
        success = true;
      } finally {
        IOUtils.closeQuietly(blockIn);
        IOUtils.closeQuietly(metaIn);
        if (!success) {
          if (reservedBytes) {
            release(length);
          }
          LOG.debug("Caching of {} was aborted.  We are now caching only {} "
                  + "bytes in total.", key, usedBytesCount.get());
          IOUtils.closeQuietly(mappableBlock);
          numBlocksFailedToCache.incrementAndGet();

          synchronized (FsDatasetCache.this) {
            mappableBlockMap.remove(key);
          }
        }
      }
    }
  }

  private class UncachingTask implements Runnable {
    private final ExtendedBlockId key; 
    private final long revocationTimeMs;

    UncachingTask(ExtendedBlockId key, long revocationDelayMs) {
      this.key = key;
      if (revocationDelayMs == 0) {
        this.revocationTimeMs = 0;
      } else {
        this.revocationTimeMs = revocationDelayMs + Time.monotonicNow();
      }
    }

    private boolean shouldDefer() {
      /* If revocationTimeMs == 0, this is an immediate uncache request.
       * No clients were anchored at the time we made the request. */
      if (revocationTimeMs == 0) {
        return false;
      }
      /* Let's check if any clients still have this block anchored. */
      boolean anchored =
        !dataset.datanode.getShortCircuitRegistry().
            processBlockMunlockRequest(key);
      if (!anchored) {
        LOG.debug("Uncaching {} now that it is no longer in use " +
            "by any clients.", key);
        return false;
      }
      long delta = revocationTimeMs - Time.monotonicNow();
      if (delta < 0) {
        LOG.warn("Forcibly uncaching {} after {} " +
            "because client(s) {} refused to stop using it.", key,
            DurationFormatUtils.formatDurationHMS(revocationTimeMs),
            dataset.datanode.getShortCircuitRegistry().getClientNames(key));
        return false;
      }
      LOG.info("Replica {} still can't be uncached because some " +
          "clients continue to use it.  Will wait for {}", key,
          DurationFormatUtils.formatDurationHMS(delta));
      return true;
    }

    @Override
    public void run() {
      Value value;

      if (shouldDefer()) {
        deferredUncachingExecutor.schedule(
            this, revocationPollingMs, TimeUnit.MILLISECONDS);
        return;
      }

      synchronized (FsDatasetCache.this) {
        value = mappableBlockMap.get(key);
      }
      Preconditions.checkNotNull(value);
      Preconditions.checkArgument(value.state == State.UNCACHING);

      IOUtils.closeQuietly(value.mappableBlock);
      synchronized (FsDatasetCache.this) {
        mappableBlockMap.remove(key);
      }
      long newUsedBytes = release(value.mappableBlock.getLength());
      numBlocksCached.addAndGet(-1);
      dataset.datanode.getMetrics().incrBlocksUncached(1);
      if (revocationTimeMs != 0) {
        LOG.debug("Uncaching of {} completed. usedBytes = {}",
            key, newUsedBytes);
      } else {
        LOG.debug("Deferred uncaching of {} completed. usedBytes = {}",
            key, newUsedBytes);
      }
    }
  }

  // Stats related methods for FSDatasetMBean

  /**
   * Get the approximate amount of cache space used.
   */
  public long getCacheUsed() {
    return usedBytesCount.get();
  }

  /**
   * Get the maximum amount of bytes we can cache.  This is a constant.
   */
  public long getCacheCapacity() {
    return maxBytes;
  }

  public long getNumBlocksFailedToCache() {
    return numBlocksFailedToCache.get();
  }

  public long getNumBlocksFailedToUncache() {
    return numBlocksFailedToUncache.get();
  }

  public long getNumBlocksCached() {
    return numBlocksCached.get();
  }

  public synchronized boolean isCached(String bpid, long blockId) {
    ExtendedBlockId block = new ExtendedBlockId(blockId, bpid);
    Value val = mappableBlockMap.get(block);
    return (val != null) && val.state.shouldAdvertise();
  }

  @VisibleForTesting
  public PmemVolumeManager getPmemManager() {
    return pmemManager;
  }
}
