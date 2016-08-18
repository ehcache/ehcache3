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
package org.ehcache.jsr107;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import javax.cache.configuration.CacheEntryListenerConfiguration;

import org.ehcache.jsr107.internal.Jsr107CacheLoaderWriter;
import org.junit.Test;
import org.mockito.internal.creation.MockSettingsImpl;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class CacheResourcesTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testRegisterDeregisterAfterClose() {
    Map<CacheEntryListenerConfiguration<Object, Object>, ListenerResources<Object, Object>> emptyMap = emptyMap();
    CacheResources<Object, Object> cacheResources = new CacheResources<Object, Object>("cache", null, null, emptyMap);
    cacheResources.closeResources(new MultiCacheException());

    try {
      cacheResources.registerCacheEntryListener(mock(CacheEntryListenerConfiguration.class));
      fail();
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      cacheResources.deregisterCacheEntryListener(mock(CacheEntryListenerConfiguration.class));
      fail();
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void closesAllResources() throws Exception {
    Jsr107CacheLoaderWriter<Object, Object> loaderWriter = mock(Jsr107CacheLoaderWriter.class, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));
    Eh107Expiry<Object, Object> expiry = mock(Eh107Expiry.class, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));
    CacheEntryListenerConfiguration<Object, Object> listenerConfiguration = mock(CacheEntryListenerConfiguration.class);
    ListenerResources<Object, Object> listenerResources = mock(ListenerResources.class);

    Map<CacheEntryListenerConfiguration<Object, Object>, ListenerResources<Object, Object>> map =
        new HashMap<CacheEntryListenerConfiguration<Object, Object>, ListenerResources<Object, Object>>();
    map.put(listenerConfiguration, listenerResources);

    CacheResources<Object, Object> cacheResources = new CacheResources<Object, Object>("cache", loaderWriter, expiry, map);
    cacheResources.closeResources(new MultiCacheException());

    verify((Closeable) loaderWriter).close();
    verify((Closeable) expiry).close();
    verify(listenerResources).close();
  }
}
