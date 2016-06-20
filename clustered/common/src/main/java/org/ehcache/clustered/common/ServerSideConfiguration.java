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
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 *
 * @author cdennis
 */
public class ServerSideConfiguration implements Serializable {
  private static final long serialVersionUID = -6203570000622687613L;

  private final String defaultServerResource;
  private final Map<String, Pool> resourcePools;

  public ServerSideConfiguration(String defaultServerResource, Map<String, Pool> resourcePools) {
    this.defaultServerResource = defaultServerResource;
    this.resourcePools = new HashMap<String, Pool>(resourcePools);
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

  public static final class Pool implements Serializable {
    private static final long serialVersionUID = 3920576607695314256L;

    private final String source;
    private final long size;

    public Pool(String source, long size) {
      this.source = source;
      this.size = size;
    }

    public long size() {
      return size;
    }

    public String source() {
      return source;
    }

    @Override
    public String toString() {
      return "[" + size() + " bytes from '" + ((source() == null) ? "<default>" : source()) + "']";
    }
  }
}
