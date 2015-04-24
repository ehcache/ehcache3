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

package org.ehcache.util;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
    assertThat(i, is(10240 - size));
  }

  <V> Object addToMap(final ConcurrentMap<Object, V> map, final V v) {
    final Object key = new Object();
    map.put(key, v);
    return key;
  }
}