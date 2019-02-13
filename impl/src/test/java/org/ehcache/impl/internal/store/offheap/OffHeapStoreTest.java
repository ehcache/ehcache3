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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.config.EvictionVeto;
import org.ehcache.core.config.store.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.internal.store.offheap.portability.AssertingOffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;

import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;

public class OffHeapStoreTest extends AbstractOffHeapStoreTest {

  @Override
  protected OffHeapStore<String, String> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super String> expiry) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      StoreConfigurationImpl<String, String> storeConfiguration = new StoreConfigurationImpl<String, String>(String.class, String.class,
          null, classLoader, expiry, null, 0, keySerializer, valueSerializer);
      OffHeapStore<String, String> offHeapStore = new OffHeapStore<String, String>(storeConfiguration, timeSource, new TestStoreEventDispatcher<String, String>(), MemoryUnit.MB.toBytes(1)) {
        @Override
        protected OffHeapValueHolderPortability<String> createValuePortability(Serializer<String> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected OffHeapStore<String, byte[]> createAndInitStore(TimeSource timeSource, Expiry<? super String, ? super byte[]> expiry, EvictionVeto<? super String, ? super byte[]> evictionVeto) {
    try {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createValueSerializer(String.class, classLoader);
      Serializer<byte[]> valueSerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);
      StoreConfigurationImpl<String, byte[]> storeConfiguration = new StoreConfigurationImpl<String, byte[]>(String.class, byte[].class,
          evictionVeto, getClass().getClassLoader(), expiry, null, 0, keySerializer, valueSerializer);
      OffHeapStore<String, byte[]> offHeapStore = new OffHeapStore<String, byte[]>(storeConfiguration, timeSource, new TestStoreEventDispatcher<String, byte[]>(),MemoryUnit.MB.toBytes(1)) {
        @Override
        protected OffHeapValueHolderPortability<byte[]> createValuePortability(Serializer<byte[]> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      OffHeapStore.Provider.init(offHeapStore);
      return offHeapStore;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  protected void destroyStore(AbstractOffHeapStore<?, ?> store) {
    OffHeapStore.Provider.close((OffHeapStore<?, ?>) store);
  }
}