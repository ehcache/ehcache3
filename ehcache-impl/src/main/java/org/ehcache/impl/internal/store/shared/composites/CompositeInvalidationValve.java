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
