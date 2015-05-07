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
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.util.IsCreated;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;


/**
 * The class tries to test some of the eventing mechanism for basic PUT operations.
 */
public class EhcacheBasicPutEventsTest extends EhcacheEventsTestBase {

  @Mock
  protected CacheLoaderWriter<String, String> cacheLoaderWriter;

  protected IsCreated<String, String> isCreated = new IsCreated<String, String>();

  @Test
  public void testPutAllArgsNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.put(null, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutKeyNotNullValueNull() {
    final Ehcache<String, String> ehcache = getEhcache();
    try {
      ehcache.put("key", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutKeyNullValueNotNull() {
    final Ehcache<String, String> ehcache = getEhcache();

    try {
      ehcache.put(null, "value");
      fail();
    } catch (NullPointerException e) {
      // expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPut() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    final Ehcache<String, String> ehcache = getEhcache();
    ehcache.put("key", "value");
    verify(cacheEventListener, times(1)).onEvent(argThat(isCreated));
  }

  @Test
  public void testPutStoreAndWriterThrow() throws Exception {
    buildStore(Collections.<String, String>emptyMap());
    doThrow(new CacheAccessException("")).when(store).compute(eq("key"), getAnyBiFunction());
    doThrow(new Exception()).when(cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicPutEventsTest");
    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }


  @Test
  public void testPutWriterThrows() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    doThrow(new Exception()).when(cacheLoaderWriter).write("key", "value");
    final Ehcache<String, String> ehcache = getEhcache(cacheLoaderWriter, "EhcacheBasicPutEventsTest");
    try {
      ehcache.put("key", "value");
      fail();
    } catch (CacheWritingException e) {
      // Expected
    }
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  @Test
  public void testPutStoreThrowsNoWriter() throws Exception {
    buildStore(Collections.singletonMap("key", "oldValue"));
    doThrow(new CacheAccessException("")).when(store).compute(eq("key"), getAnyBiFunction());

    final Ehcache<String, String> ehcache = getEhcache();

    ehcache.put("key", "value");
    verify(cacheEventListener, never()).onEvent(Matchers.<CacheEvent<String, String>>any());
  }

  private Ehcache<String, String> getEhcache() {
    return getEhcache("EhcacheBasicPutEventsTest");
  }
}