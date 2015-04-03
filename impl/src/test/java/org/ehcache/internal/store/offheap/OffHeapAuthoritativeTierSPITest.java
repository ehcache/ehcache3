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

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.internal.tier.AuthoritativeTierFactory;
import org.ehcache.internal.tier.AuthoritativeTierSPITest;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;

/**
 * @author Aurelien Broszniowski
 */
public class OffHeapAuthoritativeTierSPITest extends AuthoritativeTierSPITest<String, String> {

  @Override
  protected AuthoritativeTierFactory getAuthoritativeTierFactory() {
    return new AuthoritativeTierFactory<String, String>() {
      @Override
      public AuthoritativeTier<String, String> newTier(final AuthoritativeTier.Configuration<String, String> config) {
        return newTier(config, SystemTimeSource.INSTANCE);
      }

      @Override
      public AuthoritativeTier<String, String> newTier(final AuthoritativeTier.Configuration<String, String> config, final TimeSource timeSource) {
        JavaSerializationProvider serializationProvider = new JavaSerializationProvider();

        OffHeapStore<String, String> store = new OffHeapStore<String, String>(config, serializationProvider
            .createSerializer(String.class, config.getClassLoader()),
            serializationProvider.createSerializer(String.class, config.getClassLoader()), timeSource, MemoryUnit.MB.toBytes(1));
        store.init();
        return store;
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(final Class keyType, final Class valueType, final Comparable capacityConstraint, final EvictionVeto evictionVeto, final EvictionPrioritizer evictionPrioritizer) {
        return newConfiguration(keyType, valueType, capacityConstraint, evictionVeto, evictionPrioritizer, Expirations.noExpiration());
      }

      @Override
      public AuthoritativeTier.Configuration<String, String> newConfiguration(final Class keyType, final Class valueType, final Comparable capacityConstraint, final EvictionVeto evictionVeto, final EvictionPrioritizer evictionPrioritizer, final Expiry expiry) {
        // TODO Wire capacity constraint
        return new StoreConfigurationImpl<String, String>(keyType, valueType,
            evictionVeto, evictionPrioritizer, ClassLoader.getSystemClassLoader(), expiry, ResourcePoolsBuilder.newResourcePoolsBuilder()
            .offheap(1, MemoryUnit.MB)
            .build());
      }

      @Override
      public Class<String> getKeyType() {
        return String.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public String createKey(long seed) {
        return Long.toString(seed);
      }

      @Override
      public String createValue(long seed) {
        return Long.toString(seed);
      }
    };
  }
}
