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

import java.util.UUID;

/**
 * Represents a client's state against an {@link ClusterTierManagerActiveEntity}.
 */
public class ClientState {
  /**
   * Indicates if the client has either configured or validated with clustered store manager.
   */
  private boolean attached = false;
  private UUID clientId;

  public boolean isAttached() {
    return attached;
  }

  public UUID getClientIdentifier() {
    return clientId;
  }

  void attach(UUID clientId) {
    this.attached = true;
    this.clientId = clientId;
  }

}
