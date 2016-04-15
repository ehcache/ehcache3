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

import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Random;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ConcurrentHashMapTest {

    @Test
    public void testRandomSampleOnEmptyMap() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        assertThat(map.getEvictionCandidate(new Random(), 1, null, Eviction.<String, String>noAdvice()), nullValue());
    }

    @Test
    public void testEmptyRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        map.put("foo", "bar");
        assertThat(map.getEvictionCandidate(new Random(), 0, null, Eviction.<String, String>noAdvice()), nullValue());
    }

    @Test
    public void testOversizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        map.put("foo", "bar");
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, Eviction.<String, String>noAdvice());
        assertThat(candidate.getKey(), is("foo"));
        assertThat(candidate.getValue(), is("bar"));
    }

    @Test
    public void testUndersizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, new Comparator<String>() {
          @Override
          public int compare(String t, String t1) {
            return 0;
          }
        }, Eviction.<String, String>noAdvice());
        assertThat(candidate, notNullValue());
    }

    @Test
    public void testFullyAdvisedAgainstEvictionRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 2, null, new EvictionAdvisor<String, String>() {
            @Override
            public boolean adviseAgainstEviction(String key, String value) {
                return true;
            }
        });
        assertThat(candidate, nullValue());
    }

    @Test
    public void testSelectivelyAdvisedAgainstEvictionRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Entry<String, String> candidate = map.getEvictionCandidate(new Random(), 20, new Comparator<String>() {
          @Override
          public int compare(String t, String t1) {
            return 0;
          }
        }, new EvictionAdvisor<String, String>() {

          @Override
          public boolean adviseAgainstEviction(String key, String value) {
            return key.length() > 1;
          }
        });
        assertThat(candidate.getKey().length(), is(1));
    }

    @Test
    public void testReplaceWithWeirdBehavior() {
        ConcurrentHashMap<String, Element> elementMap = new ConcurrentHashMap<String, Element>();
        final Element initialElement = new Element("key", "foo");
        elementMap.put("key", initialElement);
        assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(true));
        assertThat(elementMap.replace("key", initialElement, new Element("key", "foo")), is(false));

        ConcurrentHashMap<String, String> stringMap = new ConcurrentHashMap<String, String>();
        final String initialString = "foo";
        stringMap.put("key", initialString);
        assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
        assertThat(stringMap.replace("key", initialString, new String(initialString)), is(true));
    }

    @Test
    public void testUsesObjectIdentityForElementsOnly() {

        final String key = "ourKey";

        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<String, Object>();

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
