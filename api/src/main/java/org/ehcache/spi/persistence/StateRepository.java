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

package org.ehcache.spi.persistence;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

/**
 * A repository allowing to preserve state in the context of a {@link org.ehcache.Cache}.
 */
public interface StateRepository {

  /**
   * Gets a named persistent map rooted in the current {@code StateRepository}.
   * <P>
   *   If the map existed already, it is returned with its content fully available.
   * </P>
   *
   * @param name the map name
   * @param keyClass concrete map key type
   * @param valueClass concrete map value type
   * @param <K> the map key type, must be {@code Serializable}
   * @param <V> the map value type, must be {@code Serializable}
   * @return a map
   */
  <K extends Serializable, V extends Serializable> ConcurrentMap<K, V> getPersistentConcurrentMap(String name, Class<K> keyClass, Class<V> valueClass);
}
