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

/**
 * Specifies a {@link ClusteredResourcePool} sharing space in a server-based clustered resource.
 * The amount of space used for {@code SharedClusteredResourcePool} is designated only in
 * server configuration and is shared among all {@code SharedClusteredResourcePool} instances
 * using the same shared resource name.
 */
public interface SharedClusteredResourcePool extends ClusteredResourcePool {

  /**
   * Identifies the server-side clustered resource shared with this resource pool.
   *
   * @return the server-side clustered resource id
   */
  String getSharedResourcePool();
}
