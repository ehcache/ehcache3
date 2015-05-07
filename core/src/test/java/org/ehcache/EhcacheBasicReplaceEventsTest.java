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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.ehcache.event.CacheEvent;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsUpdated;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;

/**
 * This class provides testing of events for basic REPLACE operations.
 */
public class EhcacheBasicReplaceEventsTest extends EhcacheEventsTestBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  protected IsUpdated<String, String> isUpdated = new IsUpdated<String, String>();

  @Test
  public void testReplaceAllArgsNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.replace(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyNotNullValueNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.replace("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplaceKeyNullValueNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.replace(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testReplace() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    final FakeCacheLoaderWriter fakeLoaderWriter = new FakeCacheLoaderWriter(Collections.singletonMap("key", "oldValue"));
    final Ehcache<String, String> ehcache = getEhcache(fakeLoaderWriter, "EhcacheBasicReplaceEventsTest");
    assertThat(ehcache.replace("key", "value"), is(equalTo("oldValue")));
    verify(cacheEventListener, times(1)).onEvent(argThat(isUpdated));
  }

  @Test
  public void testReplaceCacheWriterThrows() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    doThrow(new Exception()).when(this.cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicReplaceEventsTest");
    try {
      ehcache.replace("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  private Ehcache<String, String> getEhcache() {
    return getEhcache("EhcacheBasicReplaceEventsTest");
  }
}
