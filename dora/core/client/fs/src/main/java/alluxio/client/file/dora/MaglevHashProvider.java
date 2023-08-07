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

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.grpc.Block;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A consistent hashing algorithm implementation.
 *
 * This implementation is thread safe in lazy init and in refreshing the worker list.
 * See inline comments for thread safety guarantees and semantics.
 */
@VisibleForTesting
@ThreadSafe
public class MaglevHashProvider {
  private final int mMaxAttempts;
  private final long mWorkerInfoUpdateIntervalNs;
  private final HashFunction HASH_FUNCTION = murmur3_32_fixed();

  private final AtomicLong mLastUpdatedTimestamp = new AtomicLong(System.nanoTime());
  /**
   * Counter for how many times the map has been updated.
   */
  private final LongAdder mUpdateCount = new LongAdder();

  private final AtomicReference<List<BlockWorkerInfo>> mLastWorkerInfos =
      new AtomicReference<>(ImmutableList.of());

  /**
   * Lock to protect the lazy initialization of {@link #mLookup}.
   */
  private final Object mInitLock = new Object();











  /** Seed used to compute the lookup index. */
  private static final int  INDEX_SEED = 0xDEADBEEF;


  /**
   * As described in the related paper the lookup table size
   * should be a prime number and it should be much bigger
   * than the number of nodes (lookupSize >> maxNodes ).
   */
  private final int mLookupSize = 65537;
//  private final int lookupSize = 655373;

  /**
   * The lookup table as described in:
   * {@code https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf}
   */
  private BlockWorkerInfo[] mLookup;

  /** Maps each backend to the related permutation. */
  private Map<BlockWorkerInfo, Permutation> permutations;






  /**
   * Constructor.
   *
   * @param maxAttempts max attempts to rehash
   * @param workerListTtlMs interval between retries
   */
  public MaglevHashProvider(int maxAttempts, long workerListTtlMs) {
    mMaxAttempts = maxAttempts;
    mWorkerInfoUpdateIntervalNs = workerListTtlMs * Constants.MS_NANO;
    this.permutations = new HashMap<>();
  }

  /**
   * Finds multiple workers from the hash ring.
   *
   * @param key the key to hash on
   * @param count the expected number of workers
   * @return a list of workers following the hash ring
   */
  public List<BlockWorkerInfo> getMultiple(String key, int count) {
    Set<BlockWorkerInfo> workers = new HashSet<>();
    int attempts = 0;
    while (workers.size() < count && attempts < mMaxAttempts) {
      attempts++;
      workers.add(get(key, attempts));
    }
    return ImmutableList.copyOf(workers);
  }
  public void refresh(List<BlockWorkerInfo> workerInfos) {
    Preconditions.checkArgument(!workerInfos.isEmpty(),
        "cannot refresh hash provider with empty worker list");
    maybeInitialize(workerInfos);
    // check if the worker list has expired
    if (shouldRebuildActiveNodesMapExclusively()) {
      // thread safety is valid provided that build() takes less than
      // WORKER_INFO_UPDATE_INTERVAL_NS, so that before next update the current update has been
      // finished
      if (hasWorkerListChanged(workerInfos, mLastWorkerInfos.get())) {
        updateActiveNodes(workerInfos, mLastWorkerInfos.get());
        mLastWorkerInfos.set(workerInfos);
        mUpdateCount.increment();
      }
    }
    // otherwise, do nothing and proceed with stale worker list. on next access, the worker list
    // will have been updated by another thread
  }

  /**
   * Check whether the current map has expired and needs update.
   * If called by multiple threads concurrently, only one of the callers will get a return value
   * of true, so that the map will be updated only once. The other threads will not try to
   * update and use stale information instead.
   */
  private boolean shouldRebuildActiveNodesMapExclusively() {
    // check if the worker list has expired
    long lastUpdateTs = mLastUpdatedTimestamp.get();
    long currentTs = System.nanoTime();
    if (currentTs - lastUpdateTs > mWorkerInfoUpdateIntervalNs) {
      // use CAS to only allow one thread to actually update the timestamp
      return mLastUpdatedTimestamp.compareAndSet(lastUpdateTs, currentTs);
    }
    return false;
  }

  /**
   * Lazily initializes the hash ring.
   * Only one caller gets to initialize the map while all others are blocked.
   * After the initialization, the map must not be null.
   */
  private void maybeInitialize(List<BlockWorkerInfo> workerInfos) {
    if (mLookup == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mLookup == null) {
          build(workerInfos);
          mLastWorkerInfos.set(workerInfos);
          mLastUpdatedTimestamp.set(System.nanoTime());
        }
      }
    }
  }

  /**
   * Whether the worker list has changed.
   * @param workerInfoList
   * @param anotherWorkerInfoList
   * @return
   */
  private boolean hasWorkerListChanged(List<BlockWorkerInfo> workerInfoList,
                                       List<BlockWorkerInfo> anotherWorkerInfoList) {
    if (workerInfoList == anotherWorkerInfoList) {
      return false;
    }
    Set<WorkerNetAddress> workerAddressSet = workerInfoList.stream()
        .map(info -> info.getNetAddress()).collect(Collectors.toSet());
    Set<WorkerNetAddress> anotherWorkerAddressSet = anotherWorkerInfoList.stream()
        .map(info -> info.getNetAddress()).collect(Collectors.toSet());
    return !workerAddressSet.equals(anotherWorkerAddressSet);
  }

  /**
   * Update the active nodes.
   * @param workerInfos
   * @param lastWorkerInfos
   */
  private void updateActiveNodes(List<BlockWorkerInfo> workerInfos,
                                 List<BlockWorkerInfo> lastWorkerInfos) {
    HashSet<BlockWorkerInfo> workerInfoSet = new HashSet<>(workerInfos);
    HashSet<BlockWorkerInfo> lastWorkerInfoSet = new HashSet<>(lastWorkerInfos);
    List <BlockWorkerInfo> toRemove = new ArrayList<>();
    List <BlockWorkerInfo> toAdd = new ArrayList<>();
    // remove the workers that are no longer active
    for(BlockWorkerInfo workerInfo : lastWorkerInfoSet) {
      if (!workerInfoSet.contains(workerInfo)) {
        toRemove.add(workerInfo);
      }
    }
    // add the new workers
    for(BlockWorkerInfo workerInfo : workerInfoSet) {
      if (!lastWorkerInfoSet.contains(workerInfo)) {
        toAdd.add(workerInfo);
      }
    }
    // remove the workers that are no longer active
    remove(toRemove);
    // add the new workers
    add(toAdd);
  }

  @VisibleForTesting
  BlockWorkerInfo get(String key, int index) {
    Preconditions.checkState(mLookup != null, "Hash provider is not properly initialized");
    if (mLookup.length == 0) {
      return null;
    }

    final int id = Math.abs(hash(String.format("%s%d%d", key, index, INDEX_SEED)) % mLookup.length);
    return mLookup[id];
  }

//  @VisibleForTesting
//  List<BlockWorkerInfo> getLastWorkerInfos() {
//    return mLastWorkerInfos.get();
//  }
//
//  @VisibleForTesting
//  SortedMap<Integer, BlockWorkerInfo> getActiveNodesMap() {
//    return mActiveNodes;
//  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  private void build(
      List<BlockWorkerInfo> workerInfos) {
    Preconditions.checkArgument(!workerInfos.isEmpty(), "worker list is empty");
    mLookup = new BlockWorkerInfo[0];
    add(workerInfos);
  }

  private void add(List<BlockWorkerInfo> toAdd) {
    permutations.values().forEach( Permutation::reset );
    for( BlockWorkerInfo backend : toAdd ) {
      permutations.put( backend, newPermutation(backend) );
    }

    this.mLookup = newLookup();
  }

  private void remove(Collection<BlockWorkerInfo> toRemove) {

    toRemove.forEach( permutations::remove );
    permutations.values().forEach( Permutation::reset );

    this.mLookup = newLookup();

  }

  int hash(String key) {
    return HASH_FUNCTION.hashString(key, UTF_8).asInt();
  }

  /**
   * Creates a new permutation for the given backend.
   *
   * @param backend the source of the permutation
   * @return a new permutation
   */
  private Permutation newPermutation( BlockWorkerInfo backend )
  {

    return new Permutation( backend, mLookupSize );

  }

  /**
   * Creates a new lookup table.
   *
   * @return the new lookup table
   */
  private BlockWorkerInfo[] newLookup()
  {

    final BlockWorkerInfo[] lookup = new BlockWorkerInfo[mLookupSize];
    final AtomicInteger filled = new AtomicInteger();

    do {

      permutations.values().forEach( permutation ->
      {

        final int pos = permutation.next();
        if( lookup[pos] == null )
        { //found

          lookup[pos] = permutation.backend();
          if (filled.incrementAndGet() >= mLookupSize) {
            return;
          }
        }

      });

    }while( filled.get() < mLookupSize );

    return lookup;

  }
}

class Permutation
{

  /** Seed used to compute the state offset. */
  private static final int  OFFSET_SEED = 0xDEADBABE;

  /** Seed used to compute the state skip. */
  private static final int  SKIP_SEED = 0xDEADDEAD;


  /** The backend associated to the permutation. */
  private final BlockWorkerInfo backend;

  /** The size of the lookup table. */
  private final int size;

  /** Position where to start. */
  private final int offset;

  /** Positions to skip. */
  private final int skip;

  private final HashFunction HASH_FUNCTION = murmur3_32_fixed();


  /** The current value of the permutation. */
  private int current;

  int hash1(String key) {
    return HASH_FUNCTION.hashString(key, UTF_8).asInt();
  }

  int hash2 (String key) {
    // use XXHash
    return Hashing.crc32c().hashString(key, UTF_8).asInt();
  }

  /**
   * Constructor with parameters.
   *
   * @param backend       the backend to wrap
   * @param size          size of the lookup table
   */
  Permutation( BlockWorkerInfo backend, int size )
  {

    this.size    = size;
    this.backend = backend;

    this.offset  = hash1(String.format("%s%d",
        backend.getNetAddress().dumpMainInfo(), OFFSET_SEED)) % size;

    this.skip    = hash2(String.format("%s%d",
        backend.getNetAddress().dumpMainInfo(), SKIP_SEED)) % (size-1) + 1;

    this.current = offset;

  }

  /**
   * Returns the backend related to the current permutation.
   *
   * @return the backend related to the current permutation
   */
  BlockWorkerInfo backend()
  {

    return backend;

  }

  /**
   * Returns the next value in the permutation.
   *
   * @return the next value
   */
  int next()
  {

    final int current = this.current;
    this.current = (current + skip) % size;

    return Math.abs(current);

  }

  /**
   * Resets the permutation for the new lookup size.
   *
   */
  void reset()
  {

    this.current = offset;

  }

}