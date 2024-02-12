package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.store.shared.store.StorePartition;

import java.util.Map;

public class CompositeInvalidationListener implements CachingTier.InvalidationListener<CompositeValue<?>, CompositeValue<?>> {

  private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap;

  public CompositeInvalidationListener(Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap) {
    this.invalidationListenerMap = invalidationListenerMap;
  }

  public Map<Integer, CachingTier.InvalidationListener<?, ?>> getComposites() {
    return invalidationListenerMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void onInvalidation(CompositeValue<?> key, Store.ValueHolder<CompositeValue<?>> valueHolder) {
    CachingTier.InvalidationListener listener = invalidationListenerMap.get(key.getStoreId());
    if (listener != null) {
      listener.onInvalidation(((CompositeValue) key).getValue(), new StorePartition.DecodedValueHolder<>((Store.ValueHolder) valueHolder));
    }
  }
}
