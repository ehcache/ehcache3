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

package org.ehcache.internal.store.offheap;

import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;

public class OffHeapStoreTest extends AbstractOffHeapStoreTest {

  @Override
  protected OffHeapStore<String, String> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super String> expiry) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class, null, null, classLoader, expiry, null);
    OffHeapStore<String, String> offHeapStore = new OffHeapStore<String, String>(storeConfiguration, serializer, serializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

  @Override
  protected OffHeapStore<String, byte[]> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super byte[]> expiry, EvictionVeto<? super String, ? super byte[]> evictionVeto) {
    SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
    serializationProvider.start(null);
    ClassLoader classLoader = getClass().getClassLoader();
    Serializer<String> serializer = serializationProvider.createValueSerializer(String.class, classLoader);
    Serializer<byte[]> byteArraySerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
    StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class, evictionVeto, null, getClass().getClassLoader(), expiry, null);
    OffHeapStore<String, byte[]> offHeapStore = new OffHeapStore<String, byte[]>(storeConfiguration, serializer, byteArraySerializer, timeSource, MemoryUnit.MB.toBytes(1));
    OffHeapStore.Provider.init(offHeapStore);
    return offHeapStore;
  }

  @Override
  protected void destroyStore(AbstractOffHeapStore<?, ?> store) {
    OffHeapStore.Provider.close((OffHeapStore<?, ?>) store);
  }
}