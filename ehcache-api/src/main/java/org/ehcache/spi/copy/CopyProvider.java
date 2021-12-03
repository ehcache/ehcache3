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

package org.ehcache.spi.copy;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * A {@link Service} providing {@link Copier} instances.
 * <p>
 * The {@code CacheManager} {@link org.ehcache.spi.service.ServiceProvider obtains} an instance of this
 * {@code Service} prior to creating any {@code Cache} instances.  Before creating each {@code Cache}
 * instance, the {@code CacheManager} calls the
 * {@link #createKeyCopier(Class, Serializer, ServiceConfiguration[])} and
 * {@link #createValueCopier(Class, Serializer, ServiceConfiguration[])} methods to obtain
 * {@code Copier} instances supplied to the {@code Cache}.
 */
public interface CopyProvider extends Service {

  /**
   * Creates a key {@link Copier} with the given parameters.
   *
   * @param clazz the class of the type to copy to/from
   * @param serializer the key serializer configured for the {@code Cache} for which the {@code Copier} is
   *                   being created; may be {@code null}.  If provided, this serializer may be used
   *                   during the copy operation.
   * @param configs specific configurations
   * @param <T> the type to copy to/from
   * @return a non {@code null} {@link Copier} instance
   */
  <T> Copier<T> createKeyCopier(Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?, ?>... configs);

  /**
   * Creates a value {@link Copier} with the given parameters.
   *
   * @param clazz the class of the type to copy to/from
   * @param serializer the value serializer configured for the {@code Cache} for which the {@code Copier} is
   *                   being created; may be {@code null}.  If provided, this serializer may be used
   *                   during the copy operation.
   * @param configs specific configurations
   * @param <T> the type to copy to/from
   * @return a non {@code null} {@link Copier} instance
   */
  <T> Copier<T> createValueCopier(Class<T> clazz, Serializer<T> serializer, ServiceConfiguration<?, ?>... configs);

  /**
   * Releases the provided {@link Copier} instance.
   * If the copier instance is provided by the user, {@link java.io.Closeable#close()}
   * will not be invoked.
   *
   * @param copier the copier instance to be released
   * @throws Exception when the release fails
   */
  void releaseCopier(Copier<?> copier) throws Exception;
}
