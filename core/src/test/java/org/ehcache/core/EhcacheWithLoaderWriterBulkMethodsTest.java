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
package org.ehcache.core;

import java.util.*;
import java.util.function.Function;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.ehcache.core.EhcacheBulkMethodsTest.entry;
import static org.ehcache.core.EhcacheBulkMethodsTest.valueHolder;

/**
 * @author Abhilash
 *
 */
@SuppressWarnings("unchecked")
public class EhcacheWithLoaderWriterBulkMethodsTest {

  @Test
  public void testPutAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute((Set<Integer>) argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(invocation -> {
      Function<List<Map.Entry<Integer, String>>, Object> function =
        extractFunctionFromArgs(invocation);
      function.apply(Arrays.asList(entry(1, "one"), entry(2, "two"), entry(3, "three")));
      return null;
    });
    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);

    InternalCache<Number, CharSequence> ehcache = getCache(store, cacheLoaderWriter);
    ehcache.init();

    LinkedHashMap<Number, CharSequence> entries = new LinkedHashMap<>();
    entries.put(1, "one");
    entries.put(2, "two");
    entries.put(3, "three");
    ehcache.putAll(entries);

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).writeAll(argThat(hasItems(entry(1, "one"), entry(2, "two"), entry(3, "three"))));
  }

  private <T, R> Function<T, R> extractFunctionFromArgs(InvocationOnMock invocation) {
    return (Function<T, R>) invocation.getArguments()[1];
  }

  @Test
  public void testGetAllWithLoader() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    when(store.bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(invocation -> {
      Function<Set<? extends Number>, Object> function = extractFunctionFromArgs(invocation);
      Set<? extends Number> o = (Set<? extends Number>) invocation.getArguments()[0];
      function.apply(o);

      Map<Number, ValueHolder<CharSequence>>loaderValues = new LinkedHashMap<>();
      loaderValues.put(1, valueHolder("one"));
      loaderValues.put(2, valueHolder("two"));
      loaderValues.put(3, null);
      return loaderValues;
    });

    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);

    InternalCache<Number, CharSequence> ehcache = getCache(store, cacheLoaderWriter);
    ehcache.init();
    Map<Number, CharSequence> result = ehcache.getAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    assertThat(result, hasEntry(1, "one"));
    assertThat(result, hasEntry(2, "two"));
    assertThat(result, hasEntry(3, null));
    verify(store).bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).loadAll(argThat(hasItems(1, 2, 3)));
  }

  @Test
  public void testRemoveAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(invocation -> {
      Function<List<Map.Entry<Integer, String>>, Object> function = extractFunctionFromArgs(invocation);
      function.apply(Arrays.asList(entry(1, "one"), entry(2, "two"), entry(3, "three")));
      return null;
    });
    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);

    InternalCache<Number, CharSequence> ehcache = getCache(store, cacheLoaderWriter);
    ehcache.init();
    ehcache.removeAll(new LinkedHashSet<Number>(Arrays.asList(1, 2, 3)));

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).deleteAll(argThat(hasItems(1, 2, 3)));
  }

  protected InternalCache<Number, CharSequence> getCache(Store<Number, CharSequence> store, CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter) {
    CacheConfiguration<Number, CharSequence> cacheConfig = mock(CacheConfiguration.class);
    when(cacheConfig.getExpiryPolicy()).thenReturn(mock(ExpiryPolicy.class));
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);
    ResilienceStrategy<Number, CharSequence> resilienceStrategy = mock(ResilienceStrategy.class);
    return new EhcacheWithLoaderWriter<>(cacheConfig, store, resilienceStrategy, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheWithLoaderWriterBulkMethodsTest"));
  }

}
