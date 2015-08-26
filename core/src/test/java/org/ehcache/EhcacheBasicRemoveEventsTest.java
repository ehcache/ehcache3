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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.ehcache.event.CacheEvent;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.function.BiFunction;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsRemoved;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;

/**
 * This class provides testing of events for basic REMOVE operations.
 */
public class EhcacheBasicRemoveEventsTest extends EhcacheEventsTestBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  protected IsRemoved<String, String> isRemoved = new IsRemoved<String, String>();

  @Test
  public void testRemoveKeyNull() {
    final Ehcache<String, String> ehcache = getEhcache("EhcacheBasicRemoveEventsTest");
    try {
      ehcache.remove(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testRemove() throws Exception {
    doThrow(new CacheAccessException("")).when(store).compute(eq("key"), any(BiFunction.class));
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicRemoveEventsTest");
    ehcache.remove("key");
    verify(cacheEventListener, never()).onEvent(argThat(isRemoved));
  }

  @Test
  public void testRemoveWithCacheWritingException() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    doThrow(new Exception()).when(cacheLoaderWriter).delete("key");
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicRemoveEventsTest");
    try {
      ehcache.remove("key");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }
}
