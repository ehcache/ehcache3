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

package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.expiry.ExpiryPolicy;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

public class CompositeExpiryPolicy<K, V> implements ExpiryPolicy<CompositeValue<K>, CompositeValue<V>> {
  private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap;

  public CompositeExpiryPolicy(Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap) {
    this.expiryPolicyMap = expiryPolicyMap;
  }

  @SuppressWarnings("unchecked")
  private ExpiryPolicy<K, V> getPolicy(int id) {
    return (ExpiryPolicy<K, V>) expiryPolicyMap.get(id);
  }
  @Override
  public Duration getExpiryForCreation(CompositeValue<K> key, CompositeValue<V> value) {
    return getPolicy(key.getStoreId()).getExpiryForCreation(key.getValue(), value.getValue());
  }

  @Override
  public Duration getExpiryForAccess(CompositeValue<K> key, Supplier<? extends CompositeValue<V>> value) {
    return getPolicy(key.getStoreId()).getExpiryForAccess(key.getValue(), () -> value.get().getValue());
  }

  @Override
  public Duration getExpiryForUpdate(CompositeValue<K> key, Supplier<? extends CompositeValue<V>> oldValue, CompositeValue<V> newValue) {
    return getPolicy(key.getStoreId()).getExpiryForUpdate(key.getValue(), () -> oldValue.get().getValue(), newValue.getValue());
  }
}
