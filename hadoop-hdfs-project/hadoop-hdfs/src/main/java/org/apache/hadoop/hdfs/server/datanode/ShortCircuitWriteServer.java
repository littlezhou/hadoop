package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Time;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

class ShortCircuitWriteServer implements Runnable {
  public static final Log LOG = LogFactory.getLog(ShortCircuitWriteServer.class);

  private DataNode dataNode;
  private Configuration config;
  private volatile boolean shutdown = false;

  private static final String BLOCK_TMP_DIR = "scwtemp";
  private static final byte[] META_DATA = new byte[]{0, 1, 0, 0, 0, 2, 0};

  private String blockPoolID;
  private List<? extends FsVolumeSpi> volumes;
  int nDirs;
  File[] finalizedDirs = null;
  String[] baseDirs = null;
  String[] blockTempDirs = null;
  String[] storageIDs = null;

  private int blockIndex = 0;

  ShortCircuitWriteServer(DataNode dataNode, Configuration config) {
    this.dataNode = dataNode;
    this.config = config;
  }

    private void init() {
      FsDatasetSpi<?> fsDataset;
      boolean bIn = false;
      while (true) {
        fsDataset =  dataNode.getFSDataset();
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          bIn = true;
        }
        if (fsDataset != null) {
          break;
        }
      }

      volumes = fsDataset.getVolumes();
      //String localDirs = config.get("dfs.datanode.data.dir");
      BPOfferService[] bpos = dataNode.getAllBpOs();
      blockPoolID = bpos[0].getBlockPoolId();

      nDirs = volumes.size();
      finalizedDirs = new File[nDirs];
      baseDirs = new String[nDirs];
      blockTempDirs = new String[nDirs];
      storageIDs = new String[nDirs];

      try {
        for (int i = 0; i < volumes.size(); i++) {
          finalizedDirs[i] = volumes.get(i).getFinalizedDir(blockPoolID);
          baseDirs[i] = volumes.get(i).getBasePath();
          blockTempDirs[i] = baseDirs[i]; // + "/" + BLOCK_TMP_DIR;
          storageIDs[i] = volumes.get(i).getStorageID();
        }
      } catch (IOException e) {
        LOG.error("[SCW] Error in short circuit write internal initialization:" + e);
        shutdown = true;
      }
    }

  public void shutdownServer() {
        shutdown = true;
    }

  private void startServer(int port) {
    try {
      ServerSocketChannel ssc = ServerSocketChannel.open();
      Selector accSel = Selector.open();
      ServerSocket socket = ssc.socket();
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(port));
      ssc.configureBlocking(false);
      ssc.register(accSel, SelectionKey.OP_ACCEPT);

      while (ssc.isOpen() && !shutdown) {
        if (accSel.select(1000) > 0) {
            Iterator<SelectionKey> it = accSel.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                if (key.isAcceptable()) {
                    handleAccept(key);
                }
            }
        }
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW startServer:", e);
    }
  }

  private void handleAccept(SelectionKey key) {
    try {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel client = server.accept();
      if (client != null) {
        new Thread(new WriteHandler(client, blockIndex++)).start();
      }
    } catch (IOException e) {
        LOG.error("[SCW] Failed in SCW handleAccept:", e);
    }
  }

  @Override
  public void run() {
      init();
    startServer(8899);
  }

  class WriteHandler implements Runnable {
      public static final int BUFFER_SIZE = 1 * 1024 * 1024;



    private SocketChannel sc;
      private ByteBuffer bb;
      private String path;
      private Path filePath;

    private Path blockTempPath;
    private Path blockFinalizedPath;

    private File blockTempFile;
    private File blockMetaTempFile;
    //private File blockFinalizedFile;
    //private File  blockMetaFinalizedFile;

    private long dataLen = 0;

    private int currBlockIndex;
    private int volIndex;
    private long blockID;
    private long blockGS;



      WriteHandler(SocketChannel sc, int index) {
          this.sc = sc;
          bb = ByteBuffer.allocateDirect(BUFFER_SIZE);
        currBlockIndex = index;
        volIndex = currBlockIndex % nDirs;
      }

    @Override
    public void run() {
      try {
        addBlock();
      } catch (IOException e) {
        LOG.error("[SCW] Error in write replica: " + e);
      }
    }

    private void addBlock() throws IOException {
      long start = Time.monotonicNow();

      //datanode.notifyNamenodeReceivingBlock(block, replicaHandler.getReplica().getStorageUuid());

      writeFile();    // write block data file
      writeMetaFile();

      /*
      int renameState = 0;
      File blockFinalizedDir;
      try {
        blockFinalizedDir = DatanodeUtil.idToBlockDir(finalizedDirs[volIndex], blockID);
        blockFinalizedFile = new File(blockFinalizedDir, "blk_" + blockID);   // Block.BLOCK_FILE_PREFIX
        blockMetaFinalizedFile = new File(DatanodeUtil.getMetaName(blockFinalizedFile.getAbsolutePath(), blockGS));

        NativeIO.renameTo(blockTempFile, blockFinalizedFile);
        renameState++;
        NativeIO.renameTo(blockMetaTempFile, blockMetaFinalizedFile);
        renameState++;
      } catch (IOException e) {
        if (renameState == 1 && blockFinalizedFile.exists()) {
          blockFinalizedFile.delete();
        }
        throw e;
      } */

      ReplicaBeingWritten rbwReplica = new ReplicaBeingWritten(blockID, blockGS, volumes.get(volIndex), new File(blockTempDirs[volIndex]), 0);
      rbwReplica.setNumBytes(dataLen);

      //FinalizedReplica replica = new FinalizedReplica(blockID, blockFinalizedFile.length(), blockGS, volumes.get(volIndex), blockFinalizedDir.getAbsoluteFile());

      FinalizedReplica finalizedReplica = ((FsDatasetImpl) (dataNode.data)).finalizeScwBlock(blockPoolID, rbwReplica, blockTempFile);

      //update metrics
      dataNode.metrics.addWriteBlockOp(Time.monotonicNow() - start);
      dataNode.metrics.incrWritesFromClient(true, dataLen);

      ExtendedBlock block = new ExtendedBlock(blockPoolID, blockID, finalizedReplica.getBlockFile().length(), blockGS);
      dataNode.closeBlock(block, "", storageIDs[volIndex]);

      LOG.info("[SCW] Write block successfully: " + block);
    }

    private void writeMetaFile() throws IOException {
      blockMetaTempFile = new File(DatanodeUtil.getMetaName(blockTempFile.getAbsolutePath(), blockGS));
      try {
        FileOutputStream osMeta = new FileOutputStream(blockMetaTempFile, false);
        osMeta.write(META_DATA);
        osMeta.close();
      } catch (FileNotFoundException ne) {
        throw new IOException(ne);
      }
    }

      private void writeFile() throws IOException {
        int readed = 0;
        FileOutputStream fos = null;
        FileChannel fc = null;
        if (sc.isConnected()) {
            try {
                InputStream is = sc.socket().getInputStream();
                DataInputStream dis = new DataInputStream(is);
              blockID = dis.readLong();
              blockGS = dis.readLong();

              blockTempFile = new File(blockTempDirs[volIndex], "blk_" + blockID);

                File file = blockTempFile;
                fos = new FileOutputStream(file, false);
                fc = fos.getChannel();
                //fc = FileChannel.open(filePath, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
                LOG.debug("[SCW] Writing file " + file + " ...");

                while (true) {
                    readed = sc.read(bb);
                    if (readed > 0) {
                        bb.flip();
                        while (bb.hasRemaining()) {
                            fc.write(bb);
                        }
                        dataLen += readed;
                        bb.flip();
                    } else {
                        break;
                    }
                }

                fos.close();
                sc.close();
                LOG.debug("[SCW] Write file " + file + " finished with " + dataLen + " bytes!");
            } catch (IOException e) {
                LOG.error("[SCW] Write file " + filePath + " " + dataLen, e);
                //e.printStackTrace();
              throw e;
            }
        }
    }
  }

}
