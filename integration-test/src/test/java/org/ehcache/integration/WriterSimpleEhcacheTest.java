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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class WriterSimpleEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheWriter<? super Number, ? super CharSequence> cacheWriter;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    cacheWriter = mock(CacheWriter.class);
    when(cacheWriterFactory.createCacheWriter(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheWriter) cacheWriter);
    builder.using(cacheWriterFactory);
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimplePutWithWriter() throws Exception {
    testCache.put(1, "one");
    testCache.put(2, "two");

    verify(cacheWriter, times(1)).write(eq(1), eq("one"));
    verify(cacheWriter, times(1)).write(eq(2), eq("two"));
  }

  @Test
  public void testSimpleRemoveWithWriter() throws Exception {
    testCache.put(1, "two");
    testCache.put(2, "two");
    testCache.remove(1, "one");
    testCache.remove(2, "two");

    verify(cacheWriter, never()).delete(eq(1));
    verify(cacheWriter, times(1)).delete(eq(2));
  }

  @Test
  public void testSimpleRemove2ArgsWithWriter() throws Exception {
    testCache.put(1, "one");
    testCache.remove(1, "one");

    verify(cacheWriter, times(1)).delete(eq(1));
  }

  @Test
  public void testSimplePutIfAbsentWithWriter() throws Exception {
    testCache.putIfAbsent(1, "one");
    testCache.putIfAbsent(2, "two");
    testCache.putIfAbsent(2, "two#2");

    verify(cacheWriter, times(1)).write(eq(1), eq("one"));
    verify(cacheWriter, times(1)).write(eq(2), eq("two"));
  }

  @Test
  public void testSimpleReplace2ArgsWithWriter() throws Exception {
    testCache.put(1, "one");

    testCache.replace(1, "one#2");

    verify(cacheWriter, times(1)).write(eq(1), eq("one#2"));
  }

  @Test
  public void testSimpleReplace3ArgsWithWriter() throws Exception {
    testCache.put(1, "one");

    testCache.replace(1, "one@", "one#2");
    testCache.replace(1, "one", "one#3");

    verify(cacheWriter, times(1)).write(eq(1), eq("one#3"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSimplePutAllWithWriter() throws Exception {
    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");

    testCache.putAll(values);

    verify(cacheWriter, times(1)).writeAll(argThat(contains(entry(1, "one"))));
    verify(cacheWriter, times(1)).writeAll(argThat(contains(entry(2, "two"))));
  }

  @Test
  public void testSimpleRemoveAllWithWriter() throws Exception {
    testCache.removeAll(new HashSet<Number>(Arrays.asList(1, 2)));

    verify(cacheWriter, times(1)).deleteAll(argThat(contains(1)));
    verify(cacheWriter, times(1)).deleteAll(argThat(contains(2)));
  }

  private static Map.Entry<Number, CharSequence> entry(Number number, CharSequence charSequence) {
    return new AbstractMap.SimpleEntry<Number, CharSequence>(number, charSequence);
  }

}
