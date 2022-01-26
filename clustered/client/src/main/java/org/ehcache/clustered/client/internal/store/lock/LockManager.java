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
package org.ehcache.clustered.client.internal.store.lock;

import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.ServerStoreProxyException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.LockSuccess;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.LockMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.UnlockMessage;
import org.ehcache.clustered.common.internal.store.Chain;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.LOCK_FAILURE;

public class LockManager {

  private final ClusterTierClientEntity clientEntity;
  private final Set<Long> locksHeld = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public LockManager(ClusterTierClientEntity clientEntity) {
    this.clientEntity = clientEntity;
    clientEntity.addReconnectListener(this::reconnectListener);
  }

  void reconnectListener(ClusterTierReconnectMessage reconnectMessage) {
    reconnectMessage.addLocksHeld(locksHeld);
  }

  public Chain lock(long hash) throws TimeoutException {
    LockSuccess response = getlockResponse(hash);
    locksHeld.add(hash);
    return response.getChain();
  }

  private LockSuccess getlockResponse(long hash) throws TimeoutException {
    EhcacheEntityResponse response;
    do {
      try {
        response = clientEntity.invokeAndWaitForComplete(new LockMessage(hash), false);
      } catch (TimeoutException tme) {
        throw tme;
      } catch (Exception e) {
        throw new ServerStoreProxyException(e);
      }
      if (response == null) {
        throw new ServerStoreProxyException("Response for acquiring lock was invalid null message");
      }
    } while (response.getResponseType() == LOCK_FAILURE);
    return (LockSuccess) response;
  }

  public void unlock(long hash, boolean localonly) throws TimeoutException {
    try {
      if (!localonly) {
        clientEntity.invokeAndWaitForComplete(new UnlockMessage(hash), false);
      }
      locksHeld.remove(hash);
    } catch (TimeoutException tme) {
      throw tme;
    } catch (Exception e) {
      throw new ServerStoreProxyException(e);
    }
  }
}
