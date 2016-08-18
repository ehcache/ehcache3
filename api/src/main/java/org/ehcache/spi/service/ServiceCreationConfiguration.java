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

package org.ehcache.spi.service;

/**
 * A configuration type used when creating a {@link Service}.
 *
 * @param <T> the service type this configuration works with
 *
 */
public interface ServiceCreationConfiguration<T extends Service> {

  /**
   * Indicates which service consumes this configuration at creation.
   *
   * @return the service type
   */
  Class<T> getServiceType();
}
