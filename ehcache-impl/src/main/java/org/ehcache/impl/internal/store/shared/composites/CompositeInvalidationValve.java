package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.impl.store.HashUtils;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;

public class CompositeInvalidationValve implements AuthoritativeTier.InvalidationValve {

  private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap;

  public CompositeInvalidationValve(Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap) {
    this.invalidationValveMap = invalidationValveMap;
  }

  @Override
  public void invalidateAll() throws StoreAccessException {
    // no storeId provided, iterate through all valves
    invalidationValveMap.forEach((k, v) -> {
      try {
        v.invalidateAll();
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void invalidateAllWithHash(long keyValueHash) throws StoreAccessException {
   // no storeId provided, iterate through all valves
   invalidationValveMap.forEach((k, v) -> {
      try {
        v.invalidateAllWithHash(CompositeValue.compositeHash(k, HashUtils.longHashToInt(keyValueHash)));
      } catch (StoreAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
