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
import static org.ehcache.EhcacheBasicBulkUtil.getAltEntryMap;
import static org.ehcache.EhcacheBasicBulkUtil.getEntryMap;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.ehcache.event.CacheEvent;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.util.IsCreatedOrUpdated;
import org.junit.Test;
import org.mockito.Matchers;

/**
 * Provides events based testing for PUT_ALL operations
 */
public class EhcacheBasicPutAllEventsTest extends EhcacheEventsTestBase {

  protected IsCreatedOrUpdated<String, String> isCreatedOrUpdated = new IsCreatedOrUpdated<String, String>();

  @Test
  public void testPutAllMapNull() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.putAll(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutAllMapContainsNullKey() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));

    final Map<String, String> entries = new LinkedHashMap<String, String>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put(null, "nullKey");
      }
    }
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.putAll(entries);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutAllMapContainsNullValue() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));

    final Map<String, String> entries = new LinkedHashMap<String, String>();
    for (final Map.Entry<String, String> entry : getEntryMap(KEY_SET_A).entrySet()) {
      final String key = entry.getKey();
      entries.put(key, entry.getValue());
      if ("keyA2".equals(key)) {
        entries.put("keyA2a", null);
      }
    }
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.putAll(entries);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutAll() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    final Ehcache<String, String> ehcache = getEhcache();
    final Map<String, String> contentUpdates = getAltEntryMap("new_", fanIn(KEY_SET_A, KEY_SET_C, KEY_SET_D));
    ehcache.putAll(contentUpdates);
    int invocations = KEY_SET_A.size() + KEY_SET_C.size() + KEY_SET_D.size();
    verify(cacheEventListener, times(invocations)).onEvent(argThat(isCreatedOrUpdated));
  }

  @Test
  public void testPutAllEmptyRequestCacheAccessExceptionBeforeNoWriter() throws Exception {
    buildStore(getEntryMap(KEY_SET_A, KEY_SET_B));
    doThrow(new CacheAccessException("")).when(store)
        .bulkCompute(getAnyStringSet(), getAnyEntryIterableFunction());
    final Ehcache<String, String> ehcache = getEhcache();
    ehcache.putAll(Collections.<String, String>emptyMap());
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
    return getEhcache("EhcacheBasicPutAllEventsTest");
  }
}