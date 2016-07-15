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

package org.ehcache.clustered.common;

import java.io.Serializable;

/**
 * PoolAllocation
 */
public interface PoolAllocation extends Serializable {

  /**
   * Describes a dedicated-size allocation for clustered storage.  When using a dedicated allocation,
   * storage is allocated from the server-based resource specified.
   */
  final class Dedicated implements PoolAllocation {
    private static final long serialVersionUID = -2249181124582282204L;
    private final long size;
    private final String resourceName;

    /**
     * Create a new dedicated {@code PoolAllocation}.
     *
     * @param resourceName the server-side resource from a dedicated-size allocation is made; if {@code null},
     *                     the allocation is made from the default resource
     * @param size         the size, in bytes, of the allocation
     */
    public Dedicated(String resourceName, long size) {
      this.resourceName = resourceName;
      this.size = size;
    }

    /**
     * Gets the size, in bytes, for the dedicated allocation to make from the server-side storage resource for
     * a store configured with this {@code PoolAllocation}.
     *
     * @return the dedicated allocation size
     */
    public long getSize() {
      return size;
    }

    /**
     * Gets the name of the server-side storage resource from which allocations for a store configured
     * with this {@code PoolAllocation} are made.
     *
     * @return the server-side resource name
     */
    public String getResourceName() {
      return resourceName;
    }
  }

  /**
   * Describes a shared allocation for clustered storage.  When using a shared pool,
   * allocation requests are satisfied from the server-based shared resource pool identified.
   */
  final class Shared implements PoolAllocation {
    private static final long serialVersionUID = -5111316473831788364L;
    private final String resourcePoolName;

    /**
     * Create a new shared {@code PoolAllocation}.
     *
     * @param resourcePoolName the server-side shared resource pool from which allocations are made
     */
    public Shared(String resourcePoolName) {
      this.resourcePoolName = resourcePoolName;
    }

    /**
     * Gets the name of the server-side storage resource pool from which allocations for a store configured
     * with this {@code PoolAllocation} are sub-allocated.
     *
     * @return the server-side resource pool name
     */
    public String getResourcePoolName() {
      return resourcePoolName;
    }
  }

  /**
   * Creates a Pool Allocation which inherits the Shared or Dedicated Pool Allocation from a cache which is already configured on the server.
   */
  final class Unknown implements PoolAllocation {
    private static final long serialVersionUID = 3584540926973176260L;
  }
}
