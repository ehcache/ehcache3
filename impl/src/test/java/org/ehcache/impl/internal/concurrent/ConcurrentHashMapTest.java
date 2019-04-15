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

import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import static org.ehcache.config.Eviction.noAdvice;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ConcurrentHashMapTest {

    @Test
    public void testRemoveAllWithHash() throws Exception {
        final int totalCount = 10037;

        ConcurrentHashMap<Comparable<?>, String> map = new ConcurrentHashMap<>();

        int lastHash = 0;

        for(int i = 0; i < totalCount; i++) {
            String o = Integer.toString(i);
            lastHash = o.hashCode();
            map.put(o, "val#" + i);
        }

        Collection<Entry<Comparable<?>, String>> removed = map.removeAllWithHash(lastHash);

        assertThat(removed.size(), greaterThan(0));
        assertThat(map.size() + removed.size(), is(totalCount));
        assertRemovedEntriesAreRemoved(map, removed);
    }

    @Test
    public void testRemoveAllWithHashUsingBadHashes() throws Exception {
        final int totalCount = 10037;

        ConcurrentHashMap<Comparable<?>, String> map = new ConcurrentHashMap<>();

        for(int i = 0; i < totalCount; i++) {
            BadHashKey o = new BadHashKey(i);
            map.put(o, "val#" + i);
        }

        Collection<Map.Entry<Comparable<?>, String>> removed = map.removeAllWithHash(BadHashKey.HASH_CODE);

        assertThat(removed.size(), is(totalCount));
        assertThat(map.size(), is(0));
        assertRemovedEntriesAreRemoved(map, removed);
    }

    private void assertRemovedEntriesAreRemoved(ConcurrentHashMap<Comparable<?>, String> map, Collection<Entry<Comparable<?>, String>> removed) {
        for (Entry<Comparable<?>, String> entry : map.entrySet()) {
            assertThat(removed.contains(entry), is(false));
        }
    }
    static class BadHashKey implements Comparable<BadHashKey> {

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
            if (obj instanceof BadHashKey) {
                return ((BadHashKey) obj).value == value;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return HASH_CODE;
        }

        @Override
        public int compareTo(BadHashKey o) {
            return (value < o.value) ? -1 : ((value > o.value) ? 1 : 0);
        }

    }

    @Test
    public void testRandomSampleOnEmptyMap() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        assertThat(map.getEvictionCandidate(new Random(), 1, null, noAdvice()), nullValue());
    }

    @Test
    public void testEmptyRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        map.put("foo", "bar");
        assertThat(map.getEvictionCandidate(new Random(), 0, null, noAdvice()), nullValue());
    }

    @Test
    public void testOversizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        map.put("foo", "bar");
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, noAdvice());
        assertThat(candidate.getKey(), is("foo"));
        assertThat(candidate.getValue(), is("bar"));
    }

    @Test
    public void testUndersizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, (t, t1) -> 0, noAdvice());
        assertThat(candidate, notNullValue());
    }

    @Test
    public void testFullyAdvisedAgainstEvictionRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, (key, value) -> true);
        assertThat(candidate, nullValue());
    }

    @Test
    public void testSelectivelyAdvisedAgainstEvictionRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 20, (t, t1) -> 0, (key, value) -> key.length() > 1);
        assertThat(candidate.getKey().length(), is(1));
    }

    @Test
    public void testReplaceWithWeirdBehavior() {
        ConcurrentHashMap<String, Element> elementMap = new ConcurrentHashMap<>();
        final Element initialElement = new Element("key", "foo");
        elementMap.put("key", initialElement);
        assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(true));
        assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(false));

        ConcurrentHashMap<String, String> stringMap = new ConcurrentHashMap<>();
        final String initialString = "foo";
        stringMap.put("key", initialString);
        assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
        assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
    }

    @Test
    public void testUsesObjectIdentityForElementsOnly() {

        final String key = "ourKey";

        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

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

}
