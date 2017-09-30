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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

public class ServerSideConfiguration implements Serializable {
  private static final long serialVersionUID = -6203570000622687613L;

  private final String defaultServerResource;
  private final Map<String, Pool> resourcePools;

  public ServerSideConfiguration(Map<String, Pool> resourcePools) {
    Set<String> badPools = new HashSet<>();
    for (Map.Entry<String, Pool> e : resourcePools.entrySet()) {
      if (e.getValue().getServerResource() == null) {
        badPools.add(e.getKey());
      }
    }
    if (!badPools.isEmpty()) {
      throw new IllegalArgumentException("Pools " + badPools + " define no explicit server resource, and no default server resource was specified");
    }

    this.defaultServerResource = null;
    this.resourcePools = new HashMap<>(resourcePools);
  }

  public ServerSideConfiguration(String defaultServerResource, Map<String, Pool> resourcePools) {
    if (defaultServerResource == null) {
      throw new NullPointerException("Default server resource cannot be null");
    }

    this.defaultServerResource = defaultServerResource;
    this.resourcePools = new HashMap<>(resourcePools);
  }

  /**
   * Gets the name of the default server resource.
   *
   * @return the default server resource name; may be {@code null}
   */
  public String getDefaultServerResource() {
    return defaultServerResource;
  }

  public Map<String, Pool> getResourcePools() {
    return unmodifiableMap(resourcePools);
  }

  /**
   * The definition of a pool that can be shared by multiple caches.
   */
  public static final class Pool implements Serializable {
    private static final long serialVersionUID = 3920576607695314256L;

    private final String serverResource;
    private final long size;

    /**
     * Creates a new pool definition with the given size, consuming the given server resource.
     *
     * @param size pool size
     * @param serverResource the server resource to consume
     */
    public Pool(long size, String serverResource) {
      if (size <= 0) {
        throw new IllegalArgumentException("Pool must have a positive size");
      }
      this.size = size;
      this.serverResource = serverResource;
    }

    /**
     * Creates a new pool definition with the given size, consuming the default server resource.
     *
     * @param size pool size
     */
    public Pool(long size) {
      if (size <= 0) {
        throw new IllegalArgumentException("Pool must have a positive size");
      }
      this.size = size;
      this.serverResource = null;
    }

    /**
     * Returns the size of the pool in bytes.
     *
     * @return pool size
     */
    public long getSize() {
      return size;
    }

    /**
     * Returns the server resource consumed by this pool, or {@code null} if the default pool will be used.
     *
     * @return the server resource to consume
     */
    public String getServerResource() {
      return serverResource;
    }

    @Override
    public String toString() {
      return "[" + getSize()+ " bytes from '" + ((getServerResource()== null) ? "<default>" : getServerResource()) + "']";
    }

    @Override
    public int hashCode() {
      return (this.serverResource != null ? this.serverResource.hashCode() : 0) ^ ((int) this.size) ^ ((int) (this.size >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Pool other = (Pool) obj;
      if (this.size != other.size) {
        return false;
      }
      if ((this.serverResource == null) ? (other.serverResource != null) : !this.serverResource.equals(other.serverResource)) {
        return false;
      }
      return true;
    }
  }
}
