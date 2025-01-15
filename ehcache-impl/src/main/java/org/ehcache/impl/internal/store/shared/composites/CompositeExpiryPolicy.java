/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

public class CompositeExpiryPolicy implements ExpiryPolicy<CompositeValue<?>, CompositeValue<?>> {
  private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap;

  public CompositeExpiryPolicy(Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap) {
    this.expiryPolicyMap = expiryPolicyMap;
  }

  private ExpiryPolicy<?, ?> getPolicy(int id) {
    return expiryPolicyMap.get(id);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Duration getExpiryForCreation(CompositeValue<?> key, CompositeValue<?> value) {
    ExpiryPolicy<?, ?> expiryPolicy = getPolicy(key.getStoreId());
    if (expiryPolicy == null) {
      return Duration.ZERO;
    } else {
      return ((ExpiryPolicy) expiryPolicy).getExpiryForCreation(key.getValue(), value.getValue());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Duration getExpiryForAccess(CompositeValue<?> key, Supplier<? extends CompositeValue<?>> value) {
    ExpiryPolicy<?, ?> expiryPolicy = getPolicy(key.getStoreId());
    if (expiryPolicy == null) {
      return Duration.ZERO;
    } else {
      return ((ExpiryPolicy) expiryPolicy).getExpiryForAccess(key.getValue(), () -> value.get().getValue());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Duration getExpiryForUpdate(CompositeValue<?> key, Supplier<? extends CompositeValue<?>> oldValue, CompositeValue<?> newValue) {
    ExpiryPolicy<?, ?> expiryPolicy = getPolicy(key.getStoreId());
    if (expiryPolicy == null) {
      return Duration.ZERO;
    } else {
      return ((ExpiryPolicy) expiryPolicy).getExpiryForUpdate(key.getValue(), () -> oldValue.get().getValue(), newValue.getValue());
    }
  }
}
