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

package org.ehcache.internal.tier;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.internal.store.StoreFactory;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;

/**
 * @author Aurelien Broszniowski
 */
public interface AuthoritativeTierFactory<K, V> extends StoreFactory<K,V> {

  @Override
  AuthoritativeTier<K, V> newStore();

  @Override
  AuthoritativeTier<K, V> newStoreWithCapacity(long capacity);

  @Override
  AuthoritativeTier<K, V> newStoreWithExpiry(ExpiryPolicy<? super K, ? super V> expiry, TimeSource timeSource);

  @Override
  AuthoritativeTier<K, V> newStoreWithEvictionAdvisor(EvictionAdvisor<K, V> evictionAdvisor);

}
