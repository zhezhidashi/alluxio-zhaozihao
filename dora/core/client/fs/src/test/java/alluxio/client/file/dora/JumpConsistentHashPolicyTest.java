package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.wire.WorkerNetAddress;
import com.sun.org.apache.bcel.internal.generic.SWAP;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JumpConsistentHashPolicyTest {
  private final int NUM_WORKERS = 10;
  private final int RemoveWorkerIndex = 9;
  private final int NUM_VIRTUAL_NODES = 1000;
  private final int NUM_FILES = 100000;
  private List<BlockWorkerInfo> mBlockWorkerInfos = new ArrayList<>();
  private List<String> mFileIdList = new ArrayList<>();
  @Before
  public void setUp() {
    for(int i = 0; i < NUM_WORKERS; i++) {
      WorkerNetAddress workerAddr = new WorkerNetAddress()
          .setHost("master" + i).setRpcPort(29998).setDataPort(29999).setWebPort(30000);
      mBlockWorkerInfos.add(new BlockWorkerInfo(workerAddr, 1024, 0));
    }
    for(int i = 0; i < NUM_FILES; i++) {
      mFileIdList.add("hdfs://a/b/c" + i);
    }
  }

  @Test
  public void testConsistentHashPolicy() throws ResourceExhaustedException {
    Configuration.set(PropertyKey.USER_CONSISTENT_HASH_VIRTUAL_NODE_COUNT, NUM_VIRTUAL_NODES);
    AlluxioConfiguration conf = Configuration.global();
    WorkerLocationPolicy consistentHashPolicy = new ConsistentHashPolicy(conf);
    HashMap<BlockWorkerInfo, Integer> workerCount = new HashMap<>();

    // 保存每个文件存储到哪个worker
    List<BlockWorkerInfo> FileWorkerList = new ArrayList<>();

    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = consistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
        FileWorkerList.add(worker);
      }
    }

    // 存到list里面
    List<Integer> workerCountList = new ArrayList<>(workerCount.values());
    // 计算方差
    double variance = getVariance(workerCountList);
    System.out.println("ConsistentHashPolicy variance: " + variance);

    // 打印 workerCount
//    for(BlockWorkerInfo worker : workerCount.keySet()) {
//      System.out.println(worker.getNetAddress().getHost() + " " + workerCount.get(worker));
//    }

    // 移除一个worker
    mBlockWorkerInfos.remove(RemoveWorkerIndex);

    // 重新计算
    ConsistentHashPolicy newConsistentHashPolicy = new ConsistentHashPolicy(conf);
    workerCount.clear();
    List<BlockWorkerInfo> newFileWorkerList = new ArrayList<>();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = newConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
        newFileWorkerList.add(worker);
      }
    }

    // 打印 workerCount
//    for(BlockWorkerInfo worker : workerCount.keySet()) {
//      System.out.println(worker.getNetAddress().getHost() + " " + workerCount.get(worker));
//    }

    // 对比有多少个文件存储的Worker发生了变化
    int count = 0;
    for(int i = 0; i < FileWorkerList.size(); i++) {
      if(FileWorkerList.get(i) != newFileWorkerList.get(i)) {
        count++;
      }
    }
    System.out.println("ConsistentHashPolicy change: " + count);

    // 重新计算方差
    workerCountList.clear();
    workerCountList.addAll(workerCount.values());
    variance = getVariance(workerCountList);
    System.out.println("ConsistentHashPolicy variance: " + variance);
  }

  @Test
  public void testJumpConsistentHashPolicy() throws ResourceExhaustedException {
    WorkerLocationPolicy jumpConsistentHashPolicy = new JumpConsistentHashPolicy();
    HashMap<BlockWorkerInfo, Integer> workerCount = new HashMap<>();

    // 保存每个文件存储到哪个worker
    List<BlockWorkerInfo> FileWorkerList = new ArrayList<>();

    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
        FileWorkerList.add(worker);
      }
    }

    // 存到list里面
    List<Integer> workerCountList = new ArrayList<>(workerCount.values());
    // 计算方差
    double variance = getVariance(workerCountList);
    System.out.println("JumpConsistentHashPolicy variance: " + variance);

    // 打印 workerCount
//    for(BlockWorkerInfo worker : workerCount.keySet()) {
//      System.out.println(worker.getNetAddress().getHost() + " " + workerCount.get(worker));
//    }

    // 移除一个worker
    mBlockWorkerInfos.set(RemoveWorkerIndex, mBlockWorkerInfos.get(mBlockWorkerInfos.size() - 1));
    mBlockWorkerInfos.remove(mBlockWorkerInfos.size() - 1);
    // 重新计算
    List<BlockWorkerInfo> newFileWorkerList = new ArrayList<>();
    workerCount.clear();
    JumpConsistentHashPolicy newJumpConsistentHashPolicy = new JumpConsistentHashPolicy();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = newJumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
        newFileWorkerList.add(worker);
      }
    }

    // 对比有多少个文件存储的Worker发生了变化
    int count = 0;
    for(int i = 0; i < FileWorkerList.size(); i++) {
      if(FileWorkerList.get(i) != newFileWorkerList.get(i)) {
        count++;
      }
    }
    System.out.println("JumpConsistentHashPolicy change: " + count);

    // 重新计算方差
    workerCountList.clear();
    workerCountList.addAll(workerCount.values());
    variance = getVariance(workerCountList);
    System.out.println("JumpConsistentHashPolicy variance: " + variance);
  }

  // 计算方差
  private double getVariance(List<Integer> list) {
    double variance = 0;
    double average = 0;
    for (int i = 0; i < list.size(); i++) {
      average += list.get(i);
    }
    average /= list.size();
    for (int i = 0; i < list.size(); i++) {
      variance += Math.pow(list.get(i) - average, 2);
    }
    variance /= list.size();
    return Math.round(variance * 100) / 100.0;
  }
}
