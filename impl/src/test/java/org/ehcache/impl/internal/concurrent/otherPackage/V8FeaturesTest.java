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

package org.ehcache.impl.internal.concurrent.otherPackage;

import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.concurrent.JSR166Helper;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class V8FeaturesTest {

    @Test
    public void testCompute() throws Exception {
        ConcurrentHashMap<String, Integer> chm = new ConcurrentHashMap<String, Integer>();
        chm.put("one", 1);
        chm.put("two", 2);
        chm.put("three", 3);

        Integer result = chm.compute("three", new BiFunction<String, Integer, Integer>() {
            @Override
            public Integer apply(String s, Integer i) {
                return -i;
            }
        });
        assertThat(result, equalTo(-3));
    }

    @Test
    public void testComputeIfAbsent() throws Exception {
        ConcurrentHashMap<String, AtomicInteger> chm = new ConcurrentHashMap<String, AtomicInteger>();

        assertThat(chm.get("four"), is(nullValue()));

        chm.computeIfAbsent("four", new Function<String, AtomicInteger>() {
            @Override
            public AtomicInteger apply(String s) {
                return new AtomicInteger(0);
            }
        }).incrementAndGet();
        assertThat(chm.get("four").get(), equalTo(1));
        chm.computeIfAbsent("four", new Function<String, AtomicInteger>() {
            @Override
            public AtomicInteger apply(String s) {
                return new AtomicInteger(0);
            }
        }).incrementAndGet();
        assertThat(chm.get("four").get(), equalTo(2));
        chm.computeIfAbsent("four", new Function<String, AtomicInteger>() {
            @Override
            public AtomicInteger apply(String s) {
                return new AtomicInteger(0);
            }
        }).incrementAndGet();
        assertThat(chm.get("four").get(), equalTo(3));
    }

    @Test
    public void testComputeIfPresent() throws Exception {
        ConcurrentHashMap<String, Integer> chm = new ConcurrentHashMap<String, Integer>();
        chm.put("four", 0);

        assertThat(chm.get("four"), equalTo(0));

        chm.computeIfPresent("four", new BiFunction<String, Integer, Integer>() {
            @Override
            public Integer apply(String s, Integer i) {
                return i + 1;
            }
        });
        assertThat(chm.get("four"), equalTo(1));
        chm.computeIfPresent("four", new BiFunction<String, Integer, Integer>() {
            @Override
            public Integer apply(String s, Integer i) {
                return i + 1;
            }
        });
        assertThat(chm.get("four"), equalTo(2));
        chm.computeIfPresent("four", new BiFunction<String, Integer, Integer>() {
            @Override
            public Integer apply(String s, Integer i) {
                return i + 1;
            }
        });
        assertThat(chm.get("four"), equalTo(3));
    }

    @Test
    public void testMerge() throws Exception {
        ConcurrentHashMap<Integer, String> chm = new ConcurrentHashMap<Integer, String>();
        chm.put(1, "one");
        chm.put(2, "two");
        chm.put(3, "three");

        String result = chm.merge(1, "un", new BiFunction<String, String, String>() {
            @Override
            public String apply(String s, String s2) {
                return s + "#" + s2;
            }
        });
        assertThat(result, equalTo("one#un"));
        assertThat(chm.get(1), equalTo("one#un"));
    }

    @SuppressWarnings("serial")
    @Test
    public void testReplaceAll() throws Exception {
        ConcurrentHashMap<Integer, String> chm = new ConcurrentHashMap<Integer, String>();
        chm.put(1, "one");
        chm.put(2, "two");
        chm.put(3, "three");

        chm.replaceAll(new BiFunction<Integer, String, String>() {
            @Override
            public String apply(Integer i, String s) {
                if (i == 1) return "un";
                if (i == 2) return "deux";
                if (i == 3) return "trois";
                throw new AssertionError("did not expect this pair : " + i + ":" + s);
            }
        });
        assertEquals(new HashMap<Integer, String>() {{
            put(1, "un");
            put(2, "deux");
            put(3, "trois");
        }}, chm);
    }

    @Test
    public void testForEach() throws Exception {
        ConcurrentHashMap<String, Integer> chm = new ConcurrentHashMap<String, Integer>();
        chm.put("one", 1);
        chm.put("two", 2);
        chm.put("three", 3);
        chm.put("four", 4);
        chm.put("five", 5);
        chm.put("six", 6);
        chm.put("seven", 7);
        chm.put("eight", 8);

        final Map<String, Integer> collector = Collections.synchronizedMap(new HashMap<String, Integer>());

        chm.forEach(new JSR166Helper.BiConsumer<String, Integer>() {
            @Override
            public void accept(String s, Integer i) {
                collector.put(s, i);
            }
        });

        assertThat(chm, equalTo(collector));
    }

    @Test
    public void testParallelForEach() throws Exception {
        ConcurrentHashMap<String, Integer> chm = new ConcurrentHashMap<String, Integer>();
        chm.put("one", 1);
        chm.put("two", 2);
        chm.put("three", 3);
        chm.put("four", 4);
        chm.put("five", 5);
        chm.put("six", 6);
        chm.put("seven", 7);
        chm.put("eight", 8);

        final Map<String, Integer> collector = Collections.synchronizedMap(new HashMap<String, Integer>());

        chm.forEach(4, new JSR166Helper.BiConsumer<String, Integer>() {
            @Override
            public void accept(String s, Integer i) {
                System.out.println(s + " " + i);
                collector.put(s, i);
            }
        });

        assertThat(chm, equalTo(collector));
    }


    @Test
    public void testReduce() throws Exception {
        ConcurrentHashMap<String, Entry> chm = new ConcurrentHashMap<String, Entry>();
        chm.put("SF", new Entry("CA", "San Francisco", 20));
        chm.put("PX", new Entry("AZ", "Phoenix", 2000));
        chm.put("NY", new Entry("NY", "New York City", 1));
        chm.put("LA", new Entry("CA", "Los Angeles", 40));
        chm.put("SD", new Entry("CA", "San Diego", 50));
        chm.put("SC", new Entry("CA", "Sacramento", 30));

        Integer result = chm.reduce(4, new BiFunction<String, Entry, Integer>() {
            @Override
            public Integer apply(String s, Entry entry) {
                if (entry.state.equals("CA")) {
                    return entry.temperature;
                }
                return null;
            }
        }, new BiFunction<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer temp1, Integer temp2) {
                return (temp1 + temp2) / 2;
            }
        });

        assertThat(result, is(35));
    }

    static class Entry {
        String state;
        String city;
        int temperature;
        Entry(String state, String city, int temperature) {
            this.state = state;
            this.city = city;
            this.temperature = temperature;
        }
    }

}
