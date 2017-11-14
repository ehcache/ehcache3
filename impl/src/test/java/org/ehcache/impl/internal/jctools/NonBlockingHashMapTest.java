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
package org.ehcache.impl.internal.jctools;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.*;
import static org.ehcache.config.Eviction.noAdvice;

public class NonBlockingHashMapTest {

  private NonBlockingHashMap<Long, String> map = new NonBlockingHashMap<>();

  private Random random = ThreadLocalRandom.current();

  private Map.Entry<Long, String> getEvictionCandidate(int size) {
    return map.getEvictionCandidate(random, size, Comparator.naturalOrder(), noAdvice());
  }

  @Test
  public void getEvictionCandidate_emptyMap() throws Exception {
      Map.Entry<Long, String> entry = getEvictionCandidate(4);
      assertThat(entry).isNull();
  }

  @Test
  public void getEvictionCandidate_smallerThanSample() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");

    Map.Entry<Long, String> entry = getEvictionCandidate(4);
    assertThat(entry.getKey()).isIn(1L, 2L);
  }

  @Test
  public void getEvictionCandidate() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c");
    map.put(4L, "d");
    map.put(5L, "e");
    map.put(6L, "f");
    map.put(7L, "g");
    map.put(8L, "h");
    map.put(9L, "i");

    for (int i = 0; i < 100_000; i++) {
      Map.Entry<Long, String> entry = getEvictionCandidate(4);
      assertThat(entry).describedAs("Item " + i).isNotNull();
      assertThat(entry.getKey()).describedAs("Item " + i).isGreaterThanOrEqualTo(4);
    }
  }

  @Test
  public void getEvictionCandidate_threaded() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c");
    map.put(4L, "d");
    map.put(5L, "e");
    map.put(6L, "f");
    map.put(7L, "g");
    map.put(8L, "h");
    map.put(9L, "i");

    int numThread = Runtime.getRuntime().availableProcessors();

    CountDownLatch latch = new CountDownLatch(1 + numThread);
    CountDownLatch finalLatch = new CountDownLatch(numThread);

    for (int i = 0; i < numThread; i++) {
      new Thread(() -> {
        latch.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Should not happen");
        }

        for (int j = 0; j < 100_000; j++) {
          Map.Entry<Long, String> entry = getEvictionCandidate(4);
          assertThat(entry).isNotNull();
          assertThat(entry.getKey()).describedAs("Item " + j).isGreaterThanOrEqualTo(4);
        }
        finalLatch.countDown();
      }).start();
    }

    latch.countDown();
    finalLatch.await();

  }

  private static final Comparator<Number> EVICTION_PRIORITIZER = Comparator.comparingInt(Number::intValue);

  /**
   * This test was made to make sure the eviction will start at a place where there are
   * no valid entries around. So the algorithm will need to crawl around the array to
   * find valid entries. To make sure the sampling size is counting valid entries only,
   * not invalid ones.
   */
  @Test
  public void getEvictionCandidate_startAtEnd() {
    NonBlockingHashMap<Number, Number> map = new NonBlockingHashMap<>(128);
    map.put(8, 8);
    map.put(9, 9);
    map.put(208, 208);
    map.put(209, 209);
    map.put(308, 308);
    map.put(309, 309);
    map.put(407, 407);
    map.put(409, 409);
    map.put(509, 509);
    map.put(709, 709);

    Random random = new Random() {
      @Override
      public int nextInt() {
        return -824916563;
      }
    };

    map.getEvictionCandidate(random, 8, EVICTION_PRIORITIZER, noAdvice());
  }


  @Test
  public void mappingCount_empty() throws Exception {
    assertThat(map.mappingCount()).isEqualTo(0);
  }

  @Test
  public void mappingCount_filled() throws Exception {
    LongStream.range(0, 5).forEach(i -> map.put(i, ""));
    assertThat(map.mappingCount()).isEqualTo(5);
  }

  @Ignore // doesn't work right now, can't go above Integer.MAX_VALUE
  @Test
  public void mappingCount_filledMoreThanAInt() throws Exception {
    LongStream.range(0, Integer.MAX_VALUE + 10L).parallel().forEach(i -> {
      if(i % 100_000 == 0) System.out.println(i);
      map.put(i, "");
    });
    assertThat(map.mappingCount()).isEqualTo(Integer.MAX_VALUE + 10L);
  }

  @Test
  public void remove() {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c"); // hash is 3

    map.remove(3L);

    map.put(-4L, "c"); // hash is 3
  }

  @Test
  public void removeAllWithHash_hashNotFound() throws Exception {
    map.removeAllWithHash(55);
  }

  @Test
  public void removeAllWithHash_hashOneEntry() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c");
    map.put(4L, "d");
    map.put(5L, "e");
    map.put(6L, "f");

    int hash = Long.valueOf(3L).hashCode();

    Collection<Map.Entry<Long, String>> entries  = map.removeAllWithHash(hash);
    assertThat(entries).containsOnly(entry(3L, "c"));
    assertThat(map).doesNotContain(entry(3L, "c"));
    assertThat(map).containsOnly(entry(1L, "a"), entry(2L, "b"), entry(4L, "d"), entry(5L, "e"), entry(6L, "f"));
  }

  @Test
  public void removeAllWithHash_multipleEntriesOnHash() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c"); // hash is 3
    map.put(-4L, "d"); // hash is 3

    int hash = Long.valueOf(3L).hashCode();

    Collection<Map.Entry<Long, String>> entries  = map.removeAllWithHash(hash);
    assertThat(entries).containsOnly(entry(3L, "c"), entry(-4L, "d"));
    assertThat(map).doesNotContain(entry(3L, "c"),  entry(-4L, "d"));
    assertThat(map).containsOnly(entry(1L, "a"), entry(2L, "b"));
  }
}
