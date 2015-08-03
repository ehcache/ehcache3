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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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
import org.ehcache.util.IsUpdated;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * This class provides testing of events for basic REPLACE value operations.
 */
public class EhcacheBasicReplaceValueEventsTest extends EhcacheEventsTestBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  protected IsUpdated<String, String> isUpdated = new IsUpdated<String, String>();

  @Test
  public void testReplaceValueAllArgumentsNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace(null, null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyNotNullOthersNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace("key", null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyAndOldValueNotNullNewValueNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace("key", "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyAndNewValueNotNullOldValueNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace("key", null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyAndNewValueNullOldValueNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace(null, "oldValue", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyNullValuesNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace(null, "oldValue", "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyAndOldValueNullNewValueNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.replace(null, null, "newValue");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }


  @Test
  public void testReplaceWhenMatchingEntry() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicReplaceValueEventsTest");
    assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
    verify(cacheEventListener, times(1)).onEvent(argThat(isUpdated));
  }

  @Test
  public void testReplaceWhenStoreThrows() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    doThrow(new CacheAccessException("")).when(store)
        .compute(eq("key"), getAnyBiFunction(), getBooleanNullaryFunction());
    final Ehcache<String, String> ehcache = getEhcache();
    ehcache.replace("key", "oldValue", "newValue");
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceWithImmediatelyExpiringValue() throws Exception {
    buildStore(Collections.<String, String>singletonMap("key", "value"));
    
    Expiry<String, String> expiry = mock(Expiry.class);
    when(expiry.getExpiryForUpdate("key", "value", "new-value")).thenReturn(Duration.ZERO);
    final Ehcache<String, String> ehcache = getEhcache(expiry, "testReplaceWithImmediatelyExpiringValue");
    
    ehcache.replace("key", "value", "new-value");
    verifyZeroInteractions(cacheEventListener);
  }
  
  private Ehcache<String, String> getEhcache() {
    return getEhcache("EhcacheBasicReplaceValueEventsTest");
  }
}
