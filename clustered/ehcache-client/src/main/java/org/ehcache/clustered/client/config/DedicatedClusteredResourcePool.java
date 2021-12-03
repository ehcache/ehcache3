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

package org.ehcache.clustered.client.config;

import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.MemoryUnit;

/**
 * Specifies a {@link ClusteredResourcePool} reserving space from a server-based clustered resource.
 * A {@code FixedClusteredResourcePool} uses dedicated space on the server.
 */
public interface DedicatedClusteredResourcePool extends ClusteredResourcePool, SizedResourcePool {

  /**
   * {@inheritDoc}
   * @return {@inheritDoc}
   */
  @Override
  MemoryUnit getUnit();

  /**
   * Identifies the server-side clustered resource from which space for this pool is reserved.
   * If no server resource is identified, the resource identified in the server configuration as
   * the default is used.
   *
   * @return the server-side clustered resource id; may be {@code null}
   */
  String getFromResource();
}
