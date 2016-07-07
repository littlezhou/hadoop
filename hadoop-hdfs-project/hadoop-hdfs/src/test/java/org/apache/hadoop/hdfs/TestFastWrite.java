package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestFastWrite {

  Configuration conf;
  MiniDFSCluster cluster;
  DistributedFileSystem fs;
  int factor = 10;
  int bufferLen = 1024 * 1024;
  int fileLen = factor * bufferLen;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    //conf.set("dfs.client.read.shortcircuit","true");
    //conf.set("dfs.domain.socket.path","/home/cuixuan/dn_socket_PORT");
    conf.set("dfs.checksum.type","NULL");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fs = cluster.getFileSystem();

    try {
      Thread.sleep(5000);   // wait for datanode initialization complete
    } catch (Exception e) {
    }
  }

  @Test
  public void testFastWrite() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(fileLen);
    byte[] toWriteBytes = generateBytes(fileLen);
    buffer.put(toWriteBytes);
    buffer.flip();

    try {
      Path myFile = new Path("/test/dir/file");
      FSDataOutputStream out = fs.create(myFile, (short)2);
      out.write(buffer);
      out.close();
      assertTrue(fs.exists(myFile));

      long writenFileLen = fs.getFileStatus(myFile).getLen();
      Assert.assertEquals(fileLen, writenFileLen);

      byte[] readBytes = new byte[fileLen];
      FSDataInputStream in = fs.open(myFile);
      IOUtils.readFully(in, readBytes, 0, readBytes.length);

      Assert.assertArrayEquals(toWriteBytes, readBytes);

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFastWriteMultipleTimes() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(bufferLen);
    try {
      Path myFile = new Path("/test/dir/file");
      FSDataOutputStream out = fs.create(myFile, (short)1);
      byte[] toWriteBytesEach;
      for(int i = 0; i < factor;i++) {
        buffer.clear();
        toWriteBytesEach = generateBytes(bufferLen);
        buffer.put(toWriteBytesEach);
        buffer.flip();
        out.write(buffer);
      }
      out.close();
      assertTrue(fs.exists(myFile));

      long writenFileLen = fs.getFileStatus(myFile).getLen();
      Assert.assertEquals(fileLen, writenFileLen);
    } finally {
      cluster.shutdown();
    }
  }

  public static byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
//    for (int i = 0; i < bytes.length; i++) {
//      bytes[i] = (byte) (i & 0xff);
//    }
    new Random().nextBytes(bytes);
    return bytes;
  }
}
