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
package org.ehcache;

import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_A;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_B;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_C;
import static org.ehcache.EhcacheBasicBulkUtil.KEY_SET_D;
import static org.ehcache.EhcacheBasicBulkUtil.fanIn;
import static org.ehcache.EhcacheBasicBulkUtil.getEntryMap;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.ehcache.event.CacheEvent;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsRemoved;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;

/**
 * This class provides the events based for REMOVE_ALL operations.
 */
public class EhcacheBasicRemoveAllEventsTest extends EhcacheEventsTestBase {

  protected IsRemoved<String, String> isRemoved = new IsRemoved<String, String>();

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  @Test
  public void testRemoveAllPassingNullSet() throws Exception {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.removeAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testRemoveAllSetContainsNull() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    final Set<String> keys = new LinkedHashSet<String>();
    for (final String key : KEY_SET_A) {
      keys.add(key);
      if ("keyA2".equals(key)) {
        keys.add(null);
      }
    }
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.removeAll(keys);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testRemoveAllWithFailureHandledByResilienceStrategy() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B), Collections.singleton("keyA3"));
    final Ehcache<String, String> ehcache = getEhcache();
    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);
    int maxInvocations = KEY_SET_A.size() + KEY_SET_C.size() - 1;
    verify(cacheEventListener, atMost(maxInvocations)).onEvent(argThat(isRemoved));
  }

  @Test
  public void testRemoveAllComplete() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    final Ehcache<String, String> ehcache = getEhcache();

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    ehcache.removeAll(contentUpdates);
    int invocations = KEY_SET_A.size() + KEY_SET_C.size();
    verify(cacheEventListener, times(invocations)).onEvent(argThat(isRemoved));
  }

  @Test
  public void testRemoveAllThrowsBulkException() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    doThrow(new CacheAccessException("")).when(store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());

    final Map<String, String> originalWriterContent = getEntryMap(KEY_SET_B, KEY_SET_D);
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(originalWriterContent, KEY_SET_C);
    this.cacheLoaderWriter = spy(fakeLoaderWriter);

    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicRemoveAllEventsTest");

    final Set<String> contentUpdates = fanIn(KEY_SET_A, KEY_SET_C);
    try {
      ehcache.removeAll(contentUpdates);
      fail();
    } catch (BulkCacheWritingException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@code java.util.Set<String>}.
   *
   * @return a Mockito {@code any} matcher for {@code Set<String>}
   */
  private static Set<? extends String> getAnyStringSet() {
    return any(Set.class);
  }

  /**
   * Returns a Mockito {@code any} Matcher for a {@link org.ehcache.function.Function} over a {@code Map.Entry} {@code Iterable}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}
   */
  private static Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> getAnyEntryIterableFunction() {
    return any(Function.class);
  }

  private Ehcache<String, String> getEhcache() {
    return getEhcache("EhcacheBasicRemoveAllEventsTest");
  }
}
