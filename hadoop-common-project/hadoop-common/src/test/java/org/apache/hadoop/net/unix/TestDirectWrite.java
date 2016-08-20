package org.apache.hadoop.net.unix;

import org.junit.BeforeClass;
import org.junit.Test;
import sun.misc.Unsafe;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Created by root on 7/25/16.
 */
public class TestDirectWrite {

  public static Unsafe getUnsafeInstance() throws Exception
  {
    Field theUnsafeInstance = Unsafe.class.getDeclaredField("theUnsafe");
    theUnsafeInstance.setAccessible(true);
    return (Unsafe) theUnsafeInstance.get(Unsafe.class);
  }

  @BeforeClass
  public static void loadLib() {
//    System.load("/home/hadoopdev/hadoop/hadoop-dist/target/hadoop-2.7.2/lib/native/libhdfs.so");
//    System.load("/lib64/libaio.so.1");
  }

  @Test(timeout=2000000)
  public void perf_test_write() {
    Unsafe inst;
    try {
      inst = getUnsafeInstance();
    } catch (Exception e) {
      return;
    }

    int i;
    String path = "/mnt/ssd/tmp/filetest";
    int dataSize = 1 * 1024 *1024;
    long srcBuf = inst.allocateMemory(dataSize);
    for(i = 0; i < dataSize; i++) {
      inst.putByte(srcBuf + i, (byte) (65 + i % 26));
    }
    long dataAddr = srcBuf;
    long f =  DomainSocket.create_file(path, 64 *1024, 1);// 10L * 1024 * 1024 * 1024, 0);
    for (i = 0; i < 999999999; i++)
    {
      DomainSocket.write_file(f, dataAddr, dataSize);
    }
    DomainSocket.close_file(f);

  }

  @Test(timeout=2000000)
  public void perf_test_read() {
    Unsafe inst;
    try {
      inst = getUnsafeInstance();
    } catch (Exception e) {
      return;
    }

    int i;
    String path = "/root/CentOS-6.6-x86_64-bin-DVD2.iso";
    File file = new File(path);
    if (!file.exists()) {
      System.out.println("File " + file + " does not exist or open error!");
      return;
    }
    long fileSize = file.length();

    long readBuf = inst.allocateMemory(fileSize + 4096 * 2);
    long alignReadBuf = ((readBuf + 4096 - 1) /  4096) * 4096;

    long f =  DomainSocket.open_file(path, 512 * 1024, 1);
    if (f == 0)
    {
      System.out.print("create file error\n");
    }
    long bytesRead = DomainSocket.read_file(f, 0, alignReadBuf, fileSize);
    DomainSocket.close_file(f);

  }

  @Test(timeout=2000000)
  public void perf_test_2() {
    String path = "/home/filetest";
    int bufsize = 64 * 1024;
    int nConcurrent = 16;

    long dataAddr;
    long f;
    int dataSize = 100;
    int i;

    Unsafe inst;
    try {
      inst = getUnsafeInstance();
    } catch (Exception e) {
      return;
    }

    long srcBuf = inst.allocateMemory(dataSize);
    for(i = 0; i < dataSize; i++) {
      inst.putByte(srcBuf + i, (byte) (65 + i % 26));
    }
    dataAddr = srcBuf;



    f =  DomainSocket.create_file(path, bufsize, nConcurrent); // 10L * 1024 * 1024 * 1024, 0);
    if (f == 0)
    {
      System.out.print("create file error\n");
    }


    for (i = 0; i < 99999999; i++)
    {
      DomainSocket.write_file(f, dataAddr, dataSize);
    }
    DomainSocket.close_file(f);


    f =  DomainSocket.open_file(path, bufsize, nConcurrent);
    if (f == 0)
    {
      System.out.print("create file error\n");
    }

    File file = new File(path);
    if (!file.exists()) {
      System.out.println("File " + file + " does not exist or open error!");
      return;
    }
    long fileSize = file.length();

    long readBuf = inst.allocateMemory(dataSize + 4096 * 2);
    long alignReadBuf = ((readBuf + 4096 - 1) /  4096) * 4096;

    long bytesRead = DomainSocket.read_file(f, 0, alignReadBuf, fileSize);
    DomainSocket.close_file(f);

    if ( bytesRead != fileSize) {
      System.out.println("Error in read: " + bytesRead + "/" + fileSize);
    }

    for (i = 0; i < fileSize; i++)
    {
      byte s = inst.getByte(srcBuf + i);
      byte r = inst.getByte(alignReadBuf + i);
      if (s != r) {
        System.out.println("Corrupt data " + s + "/" + r + " at " + i);
        return;
      }
    }

    System.out.println("Test passed!!!!!!");
  }

  @Test(timeout=2000000)
  public void perf_test_merge_sort() {
    int i;
    String dir = "/home/Alltests/dirwrite/";
    String outfile = dir + "xx.result";
    int nfiles = 50;
    String[] inFiles = new String[nfiles];
    for (i = 0; i < nfiles; i++)
    {
      inFiles[i] = dir + "xx" + i;
    }
    long ret = DomainSocket.merge_sort_files(outfile, inFiles, 0, 0, 100 * 1024, 0 , 0);
    ret++;
  }


  @Test(timeout=2000000)
  public void perf_test_write_compare() {
    String path = "/home/filetest";
    File outfile = new File(path);
    if (outfile.exists()) {
      outfile.delete();
    }

    int bufsize = 64 * 1024;
    int nConcurrent = 16;

    long dataSize = 96L * 1024 * 1024;
    long towrite, written;
    int recordSize = 1000;

    long dataAddr;
    long f;

    int i;

    Unsafe inst;
    try {
      inst = getUnsafeInstance();
    } catch (Exception e) {
      return;
    }

    long srcBuf = inst.allocateMemory(recordSize);
    for (i = 0; i < recordSize; i++) {
      inst.putByte(srcBuf + i, (byte) (65 + i % 26));
    }
    dataAddr = srcBuf;

    f = DomainSocket.create_file_pa(path, bufsize, nConcurrent, 10L * 1024 * 1024 * 1024, 0);
    //f = DomainSocket.create_file(path, bufsize, nConcurrent);//, 10L * 1024 * 1024 * 1024, 0);
    if (f == 0) {
      System.out.print("create file error\n");
    }

    long lenWritten;
    for (written = 0; written < dataSize;) {
      lenWritten = DomainSocket.write_file(f, dataAddr, recordSize);
      written += lenWritten;
    }
    DomainSocket.close_file(f);

    outfile.delete();
  }
}

