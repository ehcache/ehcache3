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

package org.ehcache.spi;

import org.ehcache.spi.service.Service;

/**
 *
 * This acts as a repository for {@link Service} instances, that can be used to
 * look them up by type.
 *
 * @author Alex Snaps
 */
public interface ServiceProvider {

  /**
   * Will look up the {@link Service} of the {@code serviceType}.
   * The returned service will be started or not depending on the started state of the {@code ServiceProvider}.
   *
   * @param serviceType the {@code class} of the service being looked up
   * @param <T> The actual {@link Service} type
   * @return the service instance for {@code T} type, or {@code null} if it couldn't be located
   */
  <T extends Service> T getService(Class<T> serviceType);
}
