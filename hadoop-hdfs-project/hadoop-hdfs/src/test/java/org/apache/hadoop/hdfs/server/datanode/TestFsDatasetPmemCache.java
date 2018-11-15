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
package org.apache.hadoop.hdfs.server.datanode;

import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


@NotThreadSafe
public class TestFsDatasetPmemCache extends TestFsDatasetCache {

  static {
    LogManager.getLogger(FsDatasetCache.class).setLevel(Level.DEBUG);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    conf = new HdfsConfiguration();
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 100);
    conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        CACHE_CAPACITY);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY, 10);
    conf.set(DFS_DATANODE_CACHE_PMEM_DIR_KEY, "/mnt/pmem0");

    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    nn = cluster.getNameNode();
    fsImage = nn.getFSImage();
    dn = cluster.getDataNodes().get(0);
    fsd = dn.getFSDataset();

    spyNN = InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);
  }

  @Test
  public void testPmemConfiguration() throws Exception {
    if(cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    Configuration myConf = new HdfsConfiguration();
    myConf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        CACHE_CAPACITY);

    // No Pmem directory is set
    MiniDFSCluster myCluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(1).build();
    myCluster.waitActive();
    DataNode dataNode = myCluster.getDataNodes().get(0);
    assertNull(((FsDatasetImpl)dataNode.getFSDataset()).getCacheManager()
        .getPmemManager());
    myCluster.shutdown();

    // One Pmem directory is set
    String pmem0 = "/mnt/pmem0";
    myConf.set(DFS_DATANODE_CACHE_PMEM_DIR_KEY, pmem0);
    myCluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(1).build();
    myCluster.waitActive();
    dataNode = myCluster.getDataNodes().get(0);
    assertNotNull(((FsDatasetImpl)dataNode.getFSDataset()).getCacheManager()
        .getPmemManager().getOneLocation());
    myCluster.shutdown();

    // Two Pmem directories are set
    String pmem1 = "/mnt/pmem1";
    myConf.set(DFS_DATANODE_CACHE_PMEM_DIR_KEY, pmem0 + "," + pmem1);
    myCluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(1).build();
    myCluster.waitActive();
    dataNode = myCluster.getDataNodes().get(0);
    // Test round-robin works
    long count1 = 0, count2 = 0;
    for (int i = 0; i < 10; i++) {
      String location = ((FsDatasetImpl)dataNode.getFSDataset())
          .getCacheManager().getPmemManager().getOneLocation();
      if (location.startsWith(pmem0)) {
        count1++;
      } else if (location.startsWith(pmem1)) {
        count2++;
      } else {
        fail("Unexpected persistent storage location:" + location);
      }
    }
    assertEquals(count1, count2);
    myCluster.shutdown();
  }
}
