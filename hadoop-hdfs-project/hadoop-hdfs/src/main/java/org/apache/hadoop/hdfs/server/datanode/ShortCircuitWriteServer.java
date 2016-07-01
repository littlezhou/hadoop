package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

class ShortCircuitWriteServer implements Runnable {
    public static final Log LOG = LogFactory.getLog(ShortCircuitWriteServer.class);

  private DataNode dataNode;
  private Configuration config;

    private volatile boolean shutdown = false;

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
        List<? extends FsVolumeSpi> volumes = fsDataset.getVolumes();
        String localDirs = config.get("dfs.datanode.data.dir");
        // File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, b.getBlockId());
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
      LOG.error("Failed in SCW startServer:", e);
    }
  }

  private void handleAccept(SelectionKey key) {
    try {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel client = server.accept();
      if (client != null) {
      }
    } catch (IOException e) {
        LOG.error("Failed in SCW handleAccept:", e);
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

    private long fileIndex;

    long blockID;
    long blockGS;


      WriteHandler(SocketChannel sc) {
          this.sc = sc;
          bb = ByteBuffer.allocateDirect(BUFFER_SIZE);
      }

    @Override
    public void run() {
        writeFile();
    }

      private void updateDataNode() {
//          case PIPELINE_SETUP_CREATE:
//          replicaHandler = datanode.data.createRbw(storageType, block, allowLazyPersist);
//          datanode.notifyNamenodeReceivingBlock(
//              block, replicaHandler.getReplica().getStorageUuid());
//          break;
//          dataNode.getFSDataset().createRbw();
//          dataNode.closeBlock();
//          //update metrics
//          dataNode.metrics.addWriteBlockOp(elapsed());
//          dataNode.metrics.incrWritesFromClient(false, size);
      }
    private void updateInDataNode(int blockid) {

    }

      private void writeFile() {
        int readed = 0;
        FileOutputStream fos = null;
        FileChannel fc = null;
        long dataLen = 0;
        if (sc.isConnected()) {
            try {
                InputStream is = sc.socket().getInputStream();
                DataInputStream dis = new DataInputStream(is);
              blockID = dis.readLong();
              blockGS = dis.readLong();

                File file = new File("/tmp", path);
                fos = new FileOutputStream(file, false);
                fc = fos.getChannel();
                //fc = FileChannel.open(filePath, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
                System.out.println("Writing file " + file + " ...");

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
                //System.out.println("Write file " + file + " finished with " + dataLen + " bytes!");
            } catch (IOException e) {
                LOG.error("Write file " + filePath + " " + dataLen);
                e.printStackTrace();
            }
        }
    }
  }

}
