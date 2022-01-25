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
package org.ehcache.clustered.util;

import org.ehcache.Cache;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;

public class ThrowingResilienceStrategy implements ResilienceStrategy<Long, String> {
  @Override
  public String getFailure(Long key, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public boolean containsKeyFailure(Long key, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public void putFailure(Long key, String value, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public void removeFailure(Long key, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    throw new AssertionError("Cache op failed", e);
  }

  @Override
  public Cache.Entry<Long, String> iteratorFailure(StoreAccessException e) {
    throw new AssertionError("Cache op failed", e);
  }

  @Override
  public String putIfAbsentFailure(Long key, String value, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public boolean removeFailure(Long key, String value, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public String replaceFailure(Long key, String value, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public boolean replaceFailure(Long key, String value, String newValue, StoreAccessException e) {
    throw new AssertionError("Cache op failed for key " + key, e);
  }

  @Override
  public Map<Long, String> getAllFailure(Iterable<? extends Long> keys, StoreAccessException e) {
    throw new AssertionError("Cache op failed", e);
  }

  @Override
  public void putAllFailure(Map<? extends Long, ? extends String> entries, StoreAccessException e) {
    throw new AssertionError("Cache op failed", e);
  }

  @Override
  public void removeAllFailure(Iterable<? extends Long> keys, StoreAccessException e) {
    throw new AssertionError("Cache op failed", e);
  }
}
