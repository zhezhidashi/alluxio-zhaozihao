/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.dora;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;

import java.util.List;

/**
 * An impl of WorkerLocationPolicy.
 *
 * A policy where a file path is matched to worker(s) by a consistenct hashing algorithm.
 * The hash algorithm makes sure the same path maps to the same worker sequence.
 * On top of that, consistent hashing makes sure worker membership changes incur minimal
 * hash changes.
 */
public class DxHashPolicy implements WorkerLocationPolicy {
  private final DxHashProvider HASH_PROVIDER;
  /**
   * This is the number of virtual nodes in the consistent hashing algorithm.
   * In a consistent hashing algorithm, on membership changes, some virtual nodes are
   * re-distributed instead of rebuilding the whole hash table.
   * This guarantees the hash table is changed only in a minimal.
   * In order to achieve that, the number of virtual nodes should be X times the physical nodes
   * in the cluster, where X is a balance between redistribution granularity and size.
   */

  /**
   * Constructs a new {@link DxHashPolicy}.
   *
   * @param replicas the number of replicas
   */
  public DxHashPolicy(int replicas) {
    HASH_PROVIDER = new DxHashProvider(100, Constants.SECOND_MS, replicas);
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
                                                   String fileId, int count) throws ResourceExhaustedException {
    if (blockWorkerInfos.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Not enough workers in the cluster %d workers in the cluster but %d required",
          blockWorkerInfos.size(), count));
    }
    HASH_PROVIDER.refresh(blockWorkerInfos);
    List<BlockWorkerInfo> workers = HASH_PROVIDER.getMultiple(fileId, count);
    if (workers.size() != count) {
      throw new ResourceExhaustedException(String.format(
          "Found %d workers from the hash ring but %d required", blockWorkerInfos.size(), count));
    }
    return workers;
  }
}
