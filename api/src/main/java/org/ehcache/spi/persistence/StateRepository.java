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
import java.util.function.Predicate;

/**
 * A repository allowing to preserve state in the context of a {@link org.ehcache.Cache}.
 */
public interface StateRepository {

  /**
   * Gets a named state holder rooted in the current {@code StateRepository}.
   * <p>
   * If the state holder existed already, it is returned with its content fully available.
   *
   * @deprecated Replaced by {@link #getPersistentStateHolder(String, Class, Class, Predicate, ClassLoader)} that takes in a Predicate that authorizes a class for deserialization
   *
   * @param name       the state holder name
   * @param keyClass   concrete key type
   * @param valueClass concrete value type
   * @param <K>        the key type, must be {@code Serializable}
   * @param <V>        the value type, must be {@code Serializable}
   * @return a state holder
   */
  @Deprecated
  default <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(String name, Class<K> keyClass, Class<V> valueClass) {
    return getPersistentStateHolder(name, keyClass, valueClass, c -> true, null);
  }

  /**
   * Gets a named state holder rooted in the current {@code StateRepository}.
   * <p>
   * If the state holder existed already, it is returned with its content fully available.
   *
   * @param name the state holder name
   * @param keyClass concrete key type
   * @param valueClass concrete value type
   * @param <K> the key type, must be {@code Serializable}
   * @param <V> the value type, must be {@code Serializable}
   * @param isClassPermitted Predicate that determines whether a class is authorized for deserialization as part of key or value deserialization
   * @param classLoader class loader used at the time of deserialization of key and value
   * @return a state holder
   */
  <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(String name,
                                                                                              Class<K> keyClass,
                                                                                              Class<V> valueClass,
                                                                                              Predicate<Class<?>> isClassPermitted,
                                                                                              ClassLoader classLoader);
}
