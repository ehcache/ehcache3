package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.shared.store.StorePartition;

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
