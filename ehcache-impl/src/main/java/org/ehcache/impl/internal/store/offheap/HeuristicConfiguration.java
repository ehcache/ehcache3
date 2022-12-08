/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.impl.internal.store.offheap.MemorySizeParser;
import org.terracotta.offheapstore.util.DebuggingUtils;

import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getAdvancedBooleanConfigProperty;
import static org.ehcache.impl.internal.store.offheap.OffHeapStoreUtils.getAdvancedMemorySizeConfigProperty;

/**
 * Configuration class for sizing offheap.
 */
public class HeuristicConfiguration {

  private static final String MINIMUM_MAX_MEMORY        = "1M";
  private static final long MINIMUM_MAX_MEMORY_IN_BYTES = MemorySizeParser.parse(MINIMUM_MAX_MEMORY);

  private static final int IDEAL_MAX_SEGMENT_SIZE = 32 * 1024 * 1024;
  private static final int MAXIMUM_CHUNK_SIZE = 1 * 1024 * 1024 * 1024;
  private static final int MINIMUM_SEGMENT_COUNT = 16;
  private static final int MAXIMUM_SEGMENT_COUNT = 16 * 1024;
  private static final int MAXIMAL_SEGMENT_SIZE_RATIO = 4;
  private static final int INITIAL_SEGMENT_SIZE_RATIO = 16;
  private static final int ASSUMED_KEY_VALUE_SIZE = 1024;

  private static final int AGGRESSIVE_INITIAL_SEGMENT_SIZE_RATIO = 1;

  private final long maximumSize;

  private final int idealMaxSegmentSize;
  private final int maximumChunkSize;
  private final int minimumSegmentCount;
  private final int maximumSegmentCount;
  private final int maximalSegmentSizeRatio;
  private final int initialSegmentSizeRatio;
  private final int assumedKeyValueSize;

  public HeuristicConfiguration(long maximumSize) {
    if (maximumSize < MINIMUM_MAX_MEMORY_IN_BYTES) {
        throw new IllegalArgumentException("The value of maxBytesLocalOffHeap is less than the minimum allowed value of " + MINIMUM_MAX_MEMORY +
                ". Reconfigure maxBytesLocalOffHeap in ehcache.xml or programmatically.");
    }
    this.maximumSize = maximumSize;

    if (getAdvancedBooleanConfigProperty("aggressive", false)) {
      this.idealMaxSegmentSize = (int) getAdvancedMemorySizeConfigProperty("idealMaxSegmentSize", IDEAL_MAX_SEGMENT_SIZE);
      this.maximumChunkSize = (int) getAdvancedMemorySizeConfigProperty("maximumChunkSize", MAXIMUM_CHUNK_SIZE);
      this.minimumSegmentCount = (int) getAdvancedMemorySizeConfigProperty("minimumSegmentCount", MINIMUM_SEGMENT_COUNT);
      this.maximumSegmentCount = (int) getAdvancedMemorySizeConfigProperty("maximumSegmentCount", MAXIMUM_SEGMENT_COUNT);
      this.maximalSegmentSizeRatio = (int) getAdvancedMemorySizeConfigProperty("maximalSegmentSizeRatio", MAXIMAL_SEGMENT_SIZE_RATIO);
      this.initialSegmentSizeRatio = (int) getAdvancedMemorySizeConfigProperty("initialSegmentSizeRatio", AGGRESSIVE_INITIAL_SEGMENT_SIZE_RATIO);
      this.assumedKeyValueSize = (int) getAdvancedMemorySizeConfigProperty("assumedKeyValueSize", ASSUMED_KEY_VALUE_SIZE);
    } else {
      this.idealMaxSegmentSize = (int) getAdvancedMemorySizeConfigProperty("idealMaxSegmentSize", IDEAL_MAX_SEGMENT_SIZE);
      this.maximumChunkSize = (int) getAdvancedMemorySizeConfigProperty("maximumChunkSize", MAXIMUM_CHUNK_SIZE);
      this.minimumSegmentCount = (int) getAdvancedMemorySizeConfigProperty("minimumSegmentCount", MINIMUM_SEGMENT_COUNT);
      this.maximumSegmentCount = (int) getAdvancedMemorySizeConfigProperty("maximumSegmentCount", MAXIMUM_SEGMENT_COUNT);
      this.maximalSegmentSizeRatio = (int) getAdvancedMemorySizeConfigProperty("maximalSegmentSizeRatio", MAXIMAL_SEGMENT_SIZE_RATIO);
      this.initialSegmentSizeRatio = (int) getAdvancedMemorySizeConfigProperty("initialSegmentSizeRatio", INITIAL_SEGMENT_SIZE_RATIO);
      this.assumedKeyValueSize = (int) getAdvancedMemorySizeConfigProperty("assumedKeyValueSize", ASSUMED_KEY_VALUE_SIZE);
    }
  }

  public long getMaximumSize() {
    return maximumSize;
  }

  public int getMinimumChunkSize() {
    return (int) Math.min(maximumChunkSize, maximalSegmentSizeRatio * (getMaximumSize() / getConcurrency()));
  }

  public int getMaximumChunkSize() {
    return (int) Math.min(getMaximumSize(), maximumChunkSize);
  }

  public int getConcurrency() {
    return Integer.highestOneBit((int) Math.min(maximumSegmentCount, Math.max(minimumSegmentCount, getMaximumSize() / idealMaxSegmentSize)));
  }

  public int getInitialSegmentTableSize() {
    return Math.max(1, getSegmentDataPageSize() / assumedKeyValueSize);
  }

  public int getSegmentDataPageSize() {
    return Integer.highestOneBit((int) Math.min(getMinimumChunkSize(), getInitialSegmentCapacity() * assumedKeyValueSize));
  }

  private long getInitialSegmentCapacity() {
    return getMaximumSize() / (getConcurrency() * initialSegmentSizeRatio * (assumedKeyValueSize + 16));
  }

  @Override
  public String toString() {
    String sb = "Heuristic Configuration: \n" + "Maximum Size (specified)   : " + DebuggingUtils.toBase2SuffixedString(getMaximumSize()) + "B\n" +
                "Minimum Chunk Size         : " + DebuggingUtils.toBase2SuffixedString(getMinimumChunkSize()) + "B\n" +
                "Maximum Chunk Size         : " + DebuggingUtils.toBase2SuffixedString(getMaximumChunkSize()) + "B\n" +
                "Concurrency                : " + getConcurrency() + "\n" +
                "Initial Segment Table Size : " + DebuggingUtils.toBase2SuffixedString(getInitialSegmentTableSize()) + " slots\n" +
                "Segment Data Page Size     : " + DebuggingUtils.toBase2SuffixedString(getSegmentDataPageSize()) + "B\n";
    return sb;
  }

}
