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

package org.ehcache.core.collections;

import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ConcurrentWeakIdentityHashMapTest {

  @Test
  public void testBasicOperations() {
    final ConcurrentWeakIdentityHashMap<Integer, String> map = new ConcurrentWeakIdentityHashMap<Integer, String>();

    final Integer key = 1;
    final String firstValue = "foo";
    final String otherValue = "bar";

    assertThat(map.containsKey(key), is(false));
    assertThat(map.get(key), nullValue());
    assertThat(map.put(key, firstValue), nullValue());
    assertThat(map.containsKey(key), is(true));
    assertThat(map.putIfAbsent(key, otherValue), is(firstValue));
    assertThat(map.replace(key, otherValue, firstValue), is(false));
    assertThat(map.get(key), is(firstValue));
    assertThat(map.replace(key, firstValue, otherValue), is(true));
    assertThat(map.get(key), is(otherValue));
    assertThat(map.remove(key, firstValue), is(false));
    assertThat(map.get(key), is(otherValue));
    assertThat(map.containsKey(key), is(true));
    assertThat(map.remove(key, otherValue), is(true));
    assertThat(map.containsKey(key), is(false));
    assertThat(map.get(key), nullValue());
    assertThat(map.putIfAbsent(key, otherValue), nullValue());
    assertThat(map.get(key), is(otherValue));
    assertThat(map.remove(key), is(otherValue));
    assertThat(map.containsKey(key), is(false));
    assertThat(map.get(key), nullValue());

  }

  @Test
  public void testSizeAccountsForGCedKeys() {
    final ConcurrentWeakIdentityHashMap<Object, String> map = new ConcurrentWeakIdentityHashMap<Object, String>();

    final String v = "present";

    addToMap(map, "gone");
    map.put(1, v);
    while(map.size() > 1) {
      System.gc();
    }
    assertThat(map.values().size(), is(1));
    assertThat(map.values().iterator().next(), is(v));
  }

  @Test
  public void testRemoveAccountsForReference() {
    final ConcurrentWeakIdentityHashMap<Object, String> map = new ConcurrentWeakIdentityHashMap<Object, String>();

    final Integer key = 1;
    final String v = "present";

    map.put(key, v);
    assertThat(map.remove(key), is(v));
  }

  @Test
  public void testIteration() throws InterruptedException {
    final ConcurrentWeakIdentityHashMap<Object, Integer> map = new ConcurrentWeakIdentityHashMap<Object, Integer>();
    int i = 0;
    while(i < 10240) {
      if (i % 1024 == 0) {
        map.put(i++, i);
      } else {
        addToMap(map, i++);
      }
    }
    System.gc();
    System.gc();
    Thread.sleep(500);
    System.gc();
    int size = 0;
    // This relies on the entrySet keeping a hard ref to all keys in the map at invocation time
    for (Map.Entry<Object, Integer> entry : map.entrySet()) {
      i--;
      size = map.size();
      assertThat(entry.getKey(), notNullValue());
    }
    assertThat(i, not(is(0)));
    assertThat(size, not(is(0)));
  }

  <V> Object addToMap(final ConcurrentMap<Object, V> map, final V v) {
    final Object key = new Object();
    map.put(key, v);
    return key;
  }

  @Test
  public void testKeySetSize() throws Exception {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<String> keys = map.keySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    assertThat(keys.size(), is(3));
  }

  @Test
  public void testKeySetContainsReflectsMapChanges() throws Exception {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<String> keys = map.keySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    assertThat(keys.contains(key1), is(true));
    assertThat(keys.contains(key2), is(true));
    assertThat(keys.contains(key3), is(true));
  }

  @Test
  public void testKeySetIteratorReflectsMapChanges() {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<String> keys = map.keySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    Iterator<String> iterator = keys.iterator();
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), startsWith("key"));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), startsWith("key"));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), startsWith("key"));
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void testEntrySetSize() throws Exception {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<Map.Entry<String, String>> entries = map.entrySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    assertThat(entries.size(), is(3));
  }

  @Test
  public void testEntrySetContainsReflectsMapChanges() throws Exception {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<Map.Entry<String, String>> entries = map.entrySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    assertThat(entries.contains(new AbstractMap.SimpleEntry<String, String>("key1", "value1")), is(true));
    assertThat(entries.contains(new AbstractMap.SimpleEntry<String, String>("key1", "value1")), is(true));
    assertThat(entries.contains(new AbstractMap.SimpleEntry<String, String>("key1", "value1")), is(true));
  }

  @Test
  public void testEntrySetIteratorReflectsMapChanges() {
    ConcurrentWeakIdentityHashMap<String, String> map = new ConcurrentWeakIdentityHashMap<String, String>();
    Set<Map.Entry<String, String>> entries = map.entrySet();

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    map.put(key1, "value1");
    map.put(key2, "value2");
    map.put(key3, "value3");

    Iterator<Map.Entry<String, String>> iterator = entries.iterator();
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next().getKey(), startsWith("key"));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next().getKey(), startsWith("key"));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next().getKey(), startsWith("key"));
    assertThat(iterator.hasNext(), is(false));
  }

}
