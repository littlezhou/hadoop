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

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.PmemMappedRegion;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache.PmemVolumeManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache.PageRounder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Represents an HDFS block that is mmapped by the DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappedBlock implements MappableBlock {
  private static final Logger LOG = LoggerFactory.getLogger(FsDatasetCache
      .class);
  private static PmemVolumeManager pmemManager;
  private static FsDatasetImpl dataset;

  private long pmemMappedAddres = -1L;
  private long length;
  private String filePath = null;
  private ExtendedBlockId key;
  private PageRounder rounder;

  PmemMappedBlock(long pmemMappedAddres, long length, String filePath,
      ExtendedBlockId key) {
    assert length > 0;
    this.pmemMappedAddres = pmemMappedAddres;
    this.length = length;
    this.filePath = filePath;
    this.key = key;
    rounder = new PageRounder();
  }

  public long getLength() {
    return length;
  }

  public static void setPersistentMemoryManager(PmemVolumeManager manager) {
    pmemManager = manager;
  }

  public static void setDataset(FsDatasetImpl impl) {
    dataset = impl;
  }

  public String getCacheFilePath() {
    return this.filePath;
  }

  public void afterCache() {
    try {
      ReplicaInfo replica = dataset.getBlockReplica(key.getBlockPoolId(),
          key.getBlockId());
      replica.setCachePath(filePath);
    } catch (IOException e) {
      LOG.warn("Fail to find the replica file of PoolID = " +
          key.getBlockPoolId() + ", BlockID = " + key.getBlockId() +
          " for :" + e.getMessage());
    }
  }

  public void afterUncache() {
    try {
      ReplicaInfo replica = dataset.getBlockReplica(key.getBlockPoolId(),
          key.getBlockId());
      replica.setCachePath(null);
    } catch (IOException e) {
      LOG.warn("Fail to find the replica file of PoolID = " +
          key.getBlockPoolId() + ", BlockID = " + key.getBlockId() +
          " for :" + e.getMessage());
    }
  }

  /**
   * Load the block.
   *
   * mmap and mlock the block, and then verify its checksum.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream.  Should be positioned at the
   *                       start.  The caller must close this.
   * @param metaIn         The meta file input stream.  Should be positioned at
   *                       the start.  The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @return               The Mappable block.
   */
  public static MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException {

    PmemMappedBlock mappableBlock = null;
    PmemMappedRegion region = null;
    String filePath = null;

    FileChannel blockChannel = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      filePath = pmemManager.getOneLocation() + "/" + key.getBlockPoolId() +
          "-" + key.getBlockId();
      region = NativeIO.POSIX.Pmem.mapBlock(filePath, length);
      if (region == null) {
        throw new IOException("Fail to map the block to persistent storage.");
      }
      verifyChecksum(region, length, metaIn, blockChannel, blockFileName);
      mappableBlock = new PmemMappedBlock(region.getAddress(),
          region.getLength(), filePath, key);
      LOG.info("MappableBlock with address = " + region.getAddress() +
          ", length = " + region.getLength() + ", path = " + filePath +
          " in persistent memory");
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (region != null) {
          // unmap content from persistent memory
          NativeIO.POSIX.Pmem.unmapBlock(region.getAddress(),
              region.getLength());
          deleteMappedFile(filePath);
        }
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum. This is an I/O intensive operation.
   */
  private static void verifyChecksum(PmemMappedRegion region, long length,
      FileInputStream metaIn, FileChannel blockChannel, String blockFileName)
          throws IOException, ChecksumException {
    // Verify the checksum from the block's meta file
    // Get the DataChecksum from the meta file header
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    FileChannel metaChannel = null;
    try {
      metaChannel = metaIn.getChannel();
      if (metaChannel == null) {
        throw new IOException("Block InputStream meta file has no FileChannel.");
      }
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // Verify the checksum
      int bytesVerified = 0;
      long mappedAddress = -1L;
      if (region != null) {
        mappedAddress = region.getAddress();
      }
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF");
        assert bytesVerified % bytesPerChecksum == 0;
        int bytesRead = fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException("checksum verification failed: premature EOF");
        }
        blockBuf.flip();
        // Number of read chunks, including partial chunk at end
        int chunks = (bytesRead+bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);
        // Success
        bytesVerified += bytesRead;
        // Copy data to persistent file
        NativeIO.POSIX.Pmem.memCopy(blockBuf.array(), mappedAddress,
            region.isPmem(), bytesRead);
        mappedAddress += bytesRead;
        // Clear buffer
        blockBuf.clear();
        checksumBuf.clear();
      }
      if (region != null) {
        NativeIO.POSIX.Pmem.memSync(region);
      }
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

  /**
   * Reads bytes into a buffer until EOF or the buffer's limit is reached
   */
  private static int fillBuffer(FileChannel channel, ByteBuffer buf)
      throws IOException {
    int bytesRead = channel.read(buf);
    if (bytesRead < 0) {
      //EOF
      return bytesRead;
    }
    while (buf.remaining() > 0) {
      int n = channel.read(buf);
      if (n < 0) {
        //EOF
        return bytesRead;
      }
      bytesRead += n;
    }
    return bytesRead;
  }

  @Override
  public void close() {
    if (pmemMappedAddres != -1L) {
      LOG.info("Start to unmap file " + filePath + " with length " + length +
          " from address " + pmemMappedAddres);
      // Current libpmem will report error when pmem_unmap is called with
      // length not aligned with page size, although the length is returned by
      // pmem_map_file.
      NativeIO.POSIX.Pmem.unmapBlock(pmemMappedAddres, rounder.roundUp(length));
      pmemMappedAddres = -1L;
      deleteMappedFile(filePath);
      filePath = null;
    }
  }

  private static void deleteMappedFile(String filePath) {
    try {
      if (filePath != null) {
        boolean result = Files.deleteIfExists(Paths.get(filePath));
        if (!result) {
          LOG.error("Fail to delete mapped file " + filePath +
              " from persistent memory");
        }
      }
    } catch (Throwable e) {
      LOG.error("Fail to delete mapped file " + filePath + " for " +
          e.getMessage() + " from persistent memory");
    }
  }
}
