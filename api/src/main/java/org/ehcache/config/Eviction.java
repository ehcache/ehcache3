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
 * Utility class for getting predefined {@link EvictionAdvisor} instance.
 */
public final class Eviction {

  private static final EvictionAdvisor<?, ?> NO_ADVICE = new EvictionAdvisor<Object, Object>() {
    @Override
    public boolean adviseAgainstEviction(Object key, Object value) {
      return false;
    }
  };

  /**
   * Returns an {@link EvictionAdvisor} where no mappings are advised against eviction.
   *
   * @param <K> the key type on which this advisor applies
   * @param <V> the value type on whivh this advisor applies
   * @return an advisor where no mappings are advised against eviction
   */
  public static <K, V> EvictionAdvisor<K, V> none() {
    return (EvictionAdvisor<K, V>) NO_ADVICE;
  }

}
