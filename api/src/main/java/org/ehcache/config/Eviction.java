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

package org.ehcache.config;

/**
 * Utility class for getting predefined {@link EvictionVeto} instance.
 *
 * @author Alex Snaps
 */
public final class Eviction {

  private static final EvictionVeto<?, ?> VETO_NONE = new EvictionVeto<Object, Object>() {
    @Override
    public boolean vetoes(Object key, Object value) {
      return false;
    }
  };

  /**
   * Returns an {@link EvictionVeto} where no mappings are vetoed from eviction.
   *
   * @param <K> the key type on which this veto applies
   * @param <V> the value type on whivh this veto applies
   * @return a veto for no mappings
   */
  public static <K, V> EvictionVeto<K, V> none() {
    return (EvictionVeto<K, V>) VETO_NONE;
  }

}
