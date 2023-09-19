package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class HashBenchmark2 {
  // worker 的数量
  private final int NUM_WORKERS = 10;
  // 虚拟节点数量
  private final int NUM_VIRTUAL_NODES = 4000;
  // 常量：1亿
  private final long Yi = 100000000L;
  // 测试文件数量是 1亿，2亿，5亿，10亿
  private final long NUM_FILES = 2 * Yi;
  // worker 列表
  private List<BlockWorkerInfo> mBlockWorkerInfos = new ArrayList<>();
  // 预先随机生成的文件名，存储到了txt里面
  private String fineNamePath = "src/test/java/alluxio/client/file/dora/billion.txt";
  // 输出文件，主要是记录每个worker被分配的文件数

  private String outputFilePath = "/root/zzh/hash-test/hash2-output.txt";
  @Before
  public void setUp() {
    for(int i = 0; i < NUM_WORKERS; i++) {
      WorkerNetAddress workerAddr = new WorkerNetAddress()
          .setHost("worker" + i).setRpcPort(29998).setDataPort(29999).setWebPort(30000);
      mBlockWorkerInfos.add(new BlockWorkerInfo(workerAddr, 1024, 0));
    }
  }

  @Test
  public void testConsistentHashPolicy() throws IOException {

    long startTime = System.currentTimeMillis();

    Configuration.set(PropertyKey.USER_CONSISTENT_HASH_VIRTUAL_NODE_COUNT, NUM_VIRTUAL_NODES);
    AlluxioConfiguration conf = Configuration.global();
    WorkerLocationPolicy consistentHashPolicy = new ConsistentHashPolicy(conf);

    // 记录每个worker被分配的文件数
    HashMap<BlockWorkerInfo, Integer> workerCount = new HashMap<>();

    // 打开文件，获取每行的字符串作为文件名

    Scanner scanner = new Scanner(new File(fineNamePath));
    String filename = null;

    for(long i = 0; i < NUM_FILES; i++) {
      filename = scanner.nextLine();

      List<BlockWorkerInfo> workers = consistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, filename, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
      }
    }

    long endTime = System.currentTimeMillis();

    FileOutputStream fos = new FileOutputStream(outputFilePath);

    // 将运行时间打印到 outputFile 里面
    fos.write(("运行时间：" + (endTime - startTime) + "ms\n").getBytes());

    // 将每个worker被分配的文件数打印到 outputFile 里面，按照 worker0 ~ worker9 的顺序，打印 worker 的域名和文件数
    for(int i = 0; i < NUM_WORKERS; i++) {
      BlockWorkerInfo worker = mBlockWorkerInfos.get(i);
      fos.write((worker.getNetAddress().getHost() + " " + workerCount.get(worker) + "\n").getBytes());
    }

  }
}
