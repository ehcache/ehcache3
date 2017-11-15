package org.ehcache.impl.internal.concurrent;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.entry;
import static org.ehcache.config.Eviction.noAdvice;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

public abstract class EvictingConcurrentMapTest {

  protected EvictingConcurrentMap<Long, String> map = initMap();

  protected abstract <K, V> EvictingConcurrentMap<K, V> initMap();

  protected abstract <K, V> EvictingConcurrentMap<K, V> initMap(int size);

  @Test
  public void testRemoveAllWithHash() throws Exception {
    final int totalCount = 10037;

    EvictingConcurrentMap<Comparable<?>, String> map = initMap();

    int lastHash = 0;

    for(int i = 0; i < totalCount; i++) {
      String o = Integer.toString(i);
      lastHash = o.hashCode();
      map.put(o, "val#" + i);
    }

    Collection<Map.Entry<Comparable<?>, String>> removed = map.removeAllWithHash(lastHash);

    assertThat(removed.size(), greaterThan(0));
    assertThat(map.size() + removed.size(), is(totalCount));
    assertRemovedEntriesAreRemoved(map, removed);
  }

  @Test
  public void testRemoveAllWithHashUsingBadHashes() throws Exception {
    final int totalCount = 10037;

    EvictingConcurrentMap<Comparable<?>, String> map = initMap();

    for(int i = 0; i < totalCount; i++) {
      EvictingConcurrentMapTest.BadHashKey o = new EvictingConcurrentMapTest.BadHashKey(i);
      map.put(o, "val#" + i);
    }

    Collection<Map.Entry<Comparable<?>, String>> removed = map.removeAllWithHash(EvictingConcurrentMapTest.BadHashKey.HASH_CODE);

    assertThat(removed.size(), is(totalCount));
    assertThat(map.size(), is(0));
    assertRemovedEntriesAreRemoved(map, removed);
  }

  private void assertRemovedEntriesAreRemoved(EvictingConcurrentMap<Comparable<?>, String> map, Collection<Map.Entry<Comparable<?>, String>> removed) {
    for (Map.Entry<Comparable<?>, String> entry : map.entrySet()) {
      assertThat(removed.contains(entry), is(false));
    }
  }
  static class BadHashKey implements Comparable<EvictingConcurrentMapTest.BadHashKey> {

    static final int HASH_CODE = 42;

    private final int value;

    public BadHashKey(int value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return "BadHashKey#" + value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof EvictingConcurrentMapTest.BadHashKey) {
        return ((EvictingConcurrentMapTest.BadHashKey) obj).value == value;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return HASH_CODE;
    }

    @Override
    public int compareTo(EvictingConcurrentMapTest.BadHashKey o) {
      return (value < o.value) ? -1 : ((value > o.value) ? 1 : 0);
    }

  }

  @Test
  public void testRandomSampleOnEmptyMap() {
    EvictingConcurrentMap<String, String> map = initMap();
    assertThat(map.getEvictionCandidate(new Random(), 1, null, noAdvice()), nullValue());
  }

  @Test
  public void testEmptyRandomSample() {
    EvictingConcurrentMap<String, String> map = initMap();
    map.put("foo", "bar");
    assertThat(map.getEvictionCandidate(new Random(), 0, null, noAdvice()), nullValue());
  }

  @Test
  public void testOversizedRandomSample() {
    EvictingConcurrentMap<String, String> map = initMap();
    map.put("foo", "bar");
    Map.Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, noAdvice());
    assertThat(candidate.getKey(), is("foo"));
    assertThat(candidate.getValue(), is("bar"));
  }

  @Test
  public void testUndersizedRandomSample() {
    EvictingConcurrentMap<String, String> map = initMap();
    for (int i = 0; i < 1000; i++) {
      map.put(Integer.toString(i), Integer.toString(i));
    }
    Map.Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, (t, t1) -> 0, noAdvice());
    assertThat(candidate, notNullValue());
  }

  @Test
  public void testFullyAdvisedAgainstEvictionRandomSample() {
    EvictingConcurrentMap<String, String> map = initMap();
    for (int i = 0; i < 1000; i++) {
      map.put(Integer.toString(i), Integer.toString(i));
    }
    Map.Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, (key, value) -> true);
    assertThat(candidate, nullValue());
  }

  @Test
  public void testSelectivelyAdvisedAgainstEvictionRandomSample() {
    EvictingConcurrentMap<String, String> map = initMap();
    for (int i = 0; i < 1000; i++) {
      map.put(Integer.toString(i), Integer.toString(i));
    }
    Map.Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 20, (t, t1) -> 0, (key, value) -> key.length() > 1);
    assertThat(candidate.getKey().length(), is(1));
  }

  /**
   * Replacement should return true based on {@code equals()} not on {@code ==}
   */
  @Test
  public void testReplaceWithWeirdBehavior() {
    EvictingConcurrentMap<String, Element> elementMap = initMap();
    final Element initialElement = new Element("key", "foo");
    elementMap.put("key", initialElement);
    assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(true));
    assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(false));

    EvictingConcurrentMap<String, String> stringMap = initMap();
    final String initialString = "foo";
    stringMap.put("key", initialString);
    assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
    assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
  }

  /**
   * Removal should return true based on {@code equals()} not on {@code ==}
   */
  @Test
  public void testRemoveWithWeirdBehavior() {
    EvictingConcurrentMap<String, String> stringMap = initMap();
    String initialString = "foo";
    stringMap.put("key", initialString);

    assertThat(stringMap.remove("key", new String(initialString)), is(true));
  }

  @Test
  public void testUsesObjectIdentityForElementsOnly() {

    final String key = "ourKey";

    EvictingConcurrentMap<String, Object> map = initMap();

    String value = new String("key");
    String valueAgain = new String("key");
    map.put(key, value);
    assertThat(map.replace(key, valueAgain, valueAgain), is(true));
    assertThat(map.replace(key, value, valueAgain), is(true));

    Element elementValue = new Element(key, value);
    Element elementValueAgain = new Element(key, value);
    map.put(key, elementValue);
    assertThat(map.replace(key, elementValueAgain, elementValue), is(false));
    assertThat(map.replace(key, elementValue, elementValueAgain), is(true));
    assertThat(map.replace(key, elementValue, elementValueAgain), is(false));
    assertThat(map.replace(key, elementValueAgain, elementValue), is(true));
  }

  static class Element {
    @SuppressWarnings("unused")
    private final Object key;

    @SuppressWarnings("unused")
    private final Object value;

    Element(final Object key, final Object value) {
      this.key = key;
      this.value = value;
    }
  }

  private Random random = ThreadLocalRandom.current();

  private Map.Entry<Long, String> getEvictionCandidate(int size) {
    return map.getEvictionCandidate(random, size, Comparator.naturalOrder(), noAdvice());
  }

  @Test
  public void getEvictionCandidate_emptyMap() throws Exception {
    Map.Entry<Long, String> entry = getEvictionCandidate(4);
    Assertions.assertThat(entry).isNull();
  }

  @Test
  public void getEvictionCandidate_smallerThanSample() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");

    Map.Entry<Long, String> entry = getEvictionCandidate(4);
    Assertions.assertThat(entry.getKey()).isIn(1L, 2L);
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
      Assertions.assertThat(entry).describedAs("Item " + i).isNotNull();
      Assertions.assertThat(entry.getKey()).describedAs("Item " + i).isGreaterThanOrEqualTo(4);
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
          Assertions.assertThat(entry).isNotNull();
          Assertions.assertThat(entry.getKey()).describedAs("Item " + j).isGreaterThanOrEqualTo(4);
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
    EvictingConcurrentMap<Number, Number> map = initMap(128);
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
    Assertions.assertThat(map.mappingCount()).isEqualTo(0);
  }

  @Test
  public void mappingCount_filled() throws Exception {
    LongStream.range(0, 5).forEach(i -> map.put(i, ""));
    Assertions.assertThat(map.mappingCount()).isEqualTo(5);
  }

  @Ignore // doesn't work right now, can't go above Integer.MAX_VALUE
  @Test
  public void mappingCount_filledMoreThanAInt() throws Exception {
    LongStream.range(0, Integer.MAX_VALUE + 10L).parallel().forEach(i -> {
      if(i % 100_000 == 0) System.out.println(i);
      map.put(i, "");
    });
    Assertions.assertThat(map.mappingCount()).isEqualTo(Integer.MAX_VALUE + 10L);
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
    Assertions.assertThat(entries).containsOnly(entry(3L, "c"));
    Assertions.assertThat(map).doesNotContain(entry(3L, "c"));
    Assertions.assertThat(map).containsOnly(entry(1L, "a"), entry(2L, "b"), entry(4L, "d"), entry(5L, "e"), entry(6L, "f"));
  }

  @Test
  public void removeAllWithHash_multipleEntriesOnHash() throws Exception {
    map.put(1L, "a");
    map.put(2L, "b");
    map.put(3L, "c"); // hash is 3
    map.put(-4L, "d"); // hash is 3

    int hash = Long.valueOf(3L).hashCode();

    Collection<Map.Entry<Long, String>> entries  = map.removeAllWithHash(hash);
    Assertions.assertThat(entries).containsOnly(entry(3L, "c"), entry(-4L, "d"));
    Assertions.assertThat(map).doesNotContain(entry(3L, "c"),  entry(-4L, "d"));
    Assertions.assertThat(map).containsOnly(entry(1L, "a"), entry(2L, "b"));
  }
}
