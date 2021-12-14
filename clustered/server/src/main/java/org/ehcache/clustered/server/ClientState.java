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
package org.ehcache.clustered.server;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a client's state against an {@link EhcacheActiveEntity}.
 */
public class ClientState {
  /**
   * Indicates if the client has either configured or validated with clustered store manager.
   */
  private boolean attached = false;

  /**
   * The set of stores to which the client has attached.
   */
  private final Set<String> attachedStores = new HashSet<String>();

  public boolean isAttached() {
    return attached;
  }

  void attach() {
    this.attached = true;
  }

  boolean addStore(String storeName) {
    return this.attachedStores.add(storeName);
  }

  boolean removeStore(String storeName) {
    return this.attachedStores.remove(storeName);
  }

  public Set<String> getAttachedStores() {
    return Collections.unmodifiableSet(new HashSet<String>(this.attachedStores));
  }
}
