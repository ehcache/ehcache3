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
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.ehcache.event.CacheEvent;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsCreated;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Provides events testing for basic PUT_IF_ABSENT operations on an {@code Ehcache}.
 *
 *
 */
public class EhcacheBasicPutIfAbsentEventsTest extends EhcacheEventsTestBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  protected IsCreated<String, String> isCreated = new IsCreated<String, String>();

  @Test
  public void testPutIfAbsentAllArgsNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    
    try {
      ehcache.putIfAbsent(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutIfAbsentKeyNotNullValueNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.putIfAbsent("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutIfAbsentKeyNullValueNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.putIfAbsent(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    doThrow(new CacheAccessException("")).when(store).computeIfAbsent(eq("key"), getAnyFunction());
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicPutIfAbsentEventsTest");
    ehcache.putIfAbsent("key", "value");
    verify(cacheEventListener, never()).onEvent(argThat(isCreated));
  }

  @Test
  public void testPutIfAbsentStoreThrows() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    doThrow(new CacheAccessException("")).when(store).computeIfAbsent(eq("key"), getAnyFunction());
    final Ehcache<String, String> ehcache = getEhcache();
    ehcache.putIfAbsent("key", "value");
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutIfAbsentStoreThrowsLoaderWriterRecovers() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    doThrow(new CacheAccessException("")).when(store).computeIfAbsent(eq("key"), getAnyFunction());
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicPutIfAbsentEventsTest");
    ehcache.putIfAbsent("key", "value");
    verify(cacheEventListener, never()).onEvent(argThat(isCreated));
  }

  @Test
  public void testPutIfAbsentNewImmediatelyExpiringValue() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    
    Expiry<String, String> expiry = mock(Expiry.class);
    when(expiry.getExpiryForCreation("key", "value")).thenReturn(Duration.ZERO);
    final Ehcache<String, String> ehcache = getEhcache(expiry, "testPutIfAbsentNewImmediatelyExpiringValue");
    
    ehcache.putIfAbsent("key", "value");
    verifyZeroInteractions(cacheEventListener);
  }

  private Ehcache<String, String> getEhcache() {
    return getEhcache("EhcacheBasicPutIfAbsentEventsTest");
  }
}
