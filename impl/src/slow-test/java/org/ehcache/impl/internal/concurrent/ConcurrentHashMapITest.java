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

package org.ehcache.impl.internal.concurrent;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class ConcurrentHashMapITest {

    private static final int ENTRIES = 10000;


    @Test
    public void testRandomValuesWithObjects() {

        ConcurrentHashMap<Object, KeyHolder<Object>> map = new ConcurrentHashMap<Object, KeyHolder<Object>>();

        for(int i = 0; i < ENTRIES; i++) {
            final Object o = new Object();
            map.put(o, new KeyHolder<Object>(o));
        }

        assertThings(map);
    }

    @Test
    public void testRandomValuesWithComparable() {
        ConcurrentHashMap<Comparable<?>, KeyHolder<Object>> map = new ConcurrentHashMap<Comparable<?>, KeyHolder<Object>>();

        for(int i = 0; i < ENTRIES; i++) {
            final EvilComparableKey o = new EvilComparableKey(UUID.randomUUID().toString());
            map.put(o, new KeyHolder<Object>(o));
        }

        assertThings(map);
    }

    @Test
    public void testRandomValues() {
        ConcurrentHashMap<Object, KeyHolder<Object>> map = new ConcurrentHashMap<Object, KeyHolder<Object>>();
        final long seed = System.nanoTime();
        System.out.println("SEED: " + seed);
        final Random random = new Random(seed);

        for(int i = 0; i < ENTRIES; i++) {
            final Object o;
            switch(i % 4) {
                case 0:
                    final int hashCode = random.nextInt();
                    o = new Object() {
                        @Override
                        public int hashCode() {
                            return hashCode;
                        }
                    };
                    break;
                case 1:
                    o = new EvilKey(Integer.toString(i));
                    break;
                default:
                    o = new EvilComparableKey(Integer.toString(i));

            }
            assertThat(map.put(o, new KeyHolder<Object>(o)) == null, is(true));
        }

        for (Object o : map.keySet()) {
            assertThat(o.toString(), map.containsKey(o), is(true));
            assertThat(o.toString(), map.get(o), notNullValue());
        }

        assertThings(map);
    }

    @Test
    public void testRandomValuesWithCollisions() {
        ConcurrentHashMap<Object, KeyHolder<Object>> map = new ConcurrentHashMap<Object, KeyHolder<Object>>();

        for(int i = 0; i < ENTRIES; i++) {
            final EvilKey o = new EvilKey(UUID.randomUUID().toString());
            map.put(o, new KeyHolder<Object>(o));
        }

        assertThings(map);
    }

    @Test
    public void testActuallyWorks() throws InterruptedException {
        final long top = 100000000;
        final String key = "counter";
        final ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<String, Long>();
        map.put(key, 0L);

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for(Long val = map.get(key); val < top && map.replace(key, val, val + 1); val = map.get(key));
            }
        };

        Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors() * 2];
        for (int i = 0, threadsLength = threads.length; i < threadsLength; ) {
            threads[i] = new Thread(runnable);
            threads[i].setName("Mutation thread #" + ++i);
            threads[i - 1].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(map.get(key), is(top));

    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertThings(final ConcurrentHashMap<?, ?> map) {
        assertThat(map.size(), is(ENTRIES));

        for(int i = 0; i < 100; i ++) {
            final HashSet randomValues = new HashSet(getRandomValues(map, ENTRIES));
            assertThat(randomValues.size(), is(ENTRIES));
            for (Object randomValue : randomValues) {
                assertThat(randomValue, instanceOf(KeyHolder.class));
                final Object key = ((KeyHolder)randomValue).key;
                assertThat("Missing " + key, map.containsKey(key), is(true));
            }
        }
    }

    private static List<?> getRandomValues(Map<?, ?> map, int amount) {
        List<?> values = new ArrayList<Object>(map.values());
        Collections.shuffle(values);
        return values.subList(0, amount);
    }


    static class KeyHolder<K> {
        final K key;

        KeyHolder(final K key) {
            this.key = key;
        }
    }

    static class EvilKey {
        final String value;

        EvilKey(final String value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            return this.value.hashCode() & 1;
        }

        @Override
        public boolean equals(final Object obj) {
            return obj != null && obj.getClass() == this.getClass() && ((EvilKey)obj).value.equals(value);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " { " + value + " }";
        }
    }

    static class EvilComparableKey extends EvilKey implements Comparable<EvilComparableKey> {

        EvilComparableKey(final String value) {
            super(value);
        }

        @Override
        public int compareTo(final EvilComparableKey o) {
            return value.compareTo(o != null ? o.value : null);
        }
    }

}
