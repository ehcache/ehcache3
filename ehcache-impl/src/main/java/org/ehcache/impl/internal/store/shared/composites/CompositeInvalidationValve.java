package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;

public class CompositeInvalidationValve implements AuthoritativeTier.InvalidationValve {

  private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap;

  public CompositeInvalidationValve(Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap) {
    this.invalidationValveMap = invalidationValveMap;
  }

  @Override
  public void invalidateAll() throws StoreAccessException {
    invalidationValveMap.forEach((k, v) -> {
      try {
        v.invalidateAll();  // how to tell which storeId to use???
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void invalidateAllWithHash(long hash) throws StoreAccessException {
    invalidationValveMap.forEach((k, v) -> {
      try {
        v.invalidateAllWithHash(hash); // correct the hash
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
