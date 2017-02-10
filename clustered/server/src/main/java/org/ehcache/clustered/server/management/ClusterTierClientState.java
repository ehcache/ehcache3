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

package org.ehcache.clustered.server.management;

import java.util.UUID;

/**
 * ClusterTierClientState
 */
public class ClusterTierClientState {

  private final boolean attached;
  private final String storeIdentifier;
  private final UUID clientId;


  public ClusterTierClientState(String storeIdentifier, boolean attached) {
    this(storeIdentifier, attached, null);
  }

  public ClusterTierClientState(String storeIdentifier, boolean attached, UUID clientId) {
    this.attached = attached;
    this.storeIdentifier = storeIdentifier;
    this.clientId = clientId;
  }

  public boolean isAttached() {
    return attached;
  }

  public String getStoreIdentifier() {
    return storeIdentifier;
  }

  public UUID getClientIdentifier() {
    return clientId;
  }
}
