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
package org.ehcache.spi.serialization;

import org.ehcache.spi.persistence.StateRepository;

/**
 * Implementations of this interface can have their state maintained in a {@code StateRepository}.
 * The state will be maintained by the authoritative tier of the cache for which this is configured.
 * <p>
 * Implementations must be thread-safe.
 * <p>
 * When used within the default serialization provider, there is an additional constructor requirement.
 * The implementations must define a constructor that takes in a {@code ClassLoader}.
 * Post instantiation, the state repository will be injected with the {@code init} method invocation.
 * This is guaranteed to happen before any serialization/deserialization interaction.
 *
 * @param <T> the type of the instances to serialize
 *
 * @see Serializer
 */
public interface StatefulSerializer<T> extends Serializer<T> {

  /**
   * This method is used to inject a {@code StateRepository} to the serializer
   * by the authoritative tier of a cache during the cache initialization.
   * The passed in state repository will have the persistent properties of the injecting tier.
   *
   * @param stateRepository the state repository
   */
  void init(StateRepository stateRepository);
}
