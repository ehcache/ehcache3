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

package org.ehcache.internal.concurrent;

import org.junit.Test;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Random;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class ConcurrentHashMapTest {

    @Test
    public void testRandomSampleOnEmptyMap() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        assertThat(map.getRandomValues(new Random(), 1, Eviction.<String, String>none()), empty());
    }
    
    @Test
    public void testEmptyRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        map.put("foo", "bar");
        assertThat(map.getRandomValues(new Random(), 0, Eviction.<String, String>none()), empty());
    }
    
    @Test
    public void testOversizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        map.put("foo", "bar");
        Collection<Entry<String, String>> sample = map.getRandomValues(new Random(), 2, Eviction.<String, String>none());
        assertThat(sample, hasSize(1));
        Entry<String, String> e = sample.iterator().next();
        assertThat(e.getKey(), is("foo"));
        assertThat(e.getValue(), is("bar"));
    }
    
    @Test
    public void testUndersizedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Collection<Entry<String, String>> sample = map.getRandomValues(new Random(), 2, Eviction.<String, String>none());
        assertThat(sample, hasSize(greaterThanOrEqualTo(2)));
    }
    
    @Test
    public void testFullyVetoedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Collection<Entry<String, String>> sample = map.getRandomValues(new Random(), 2, new EvictionVeto<String, String>() {
            @Override
            public boolean vetoes(String key, String value) {
                return true;
            }
        });
        assertThat(sample, empty());
    }
    
    @Test
    public void testSelectivelyVetoedRandomSample() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
        for (int i = 0; i < 1000; i++) {
          map.put(Integer.toString(i), Integer.toString(i));
        }
        Collection<Entry<String, String>> sample = map.getRandomValues(new Random(), 20, new EvictionVeto<String, String>() {

          @Override
          public boolean vetoes(String key, String value) {
            return key.length() > 1;
          }
        });
        assertThat(sample, hasSize(10));
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
