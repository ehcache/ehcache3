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
package org.ehcache.clustered.lock.server;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockOperation;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockTransition;

import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.StateDumpCollector;

/**
 *
 * @author cdennis
 */
class VoltronReadWriteLockActiveEntity implements ActiveServerEntity<LockOperation, LockTransition> {

  private final ClientCommunicator communicator;

  private final Set<ClientDescriptor> releaseListeners = new CopyOnWriteArraySet<>();
  private final Set<ClientDescriptor> sharedHolders = new CopyOnWriteArraySet<>();

  private volatile ClientDescriptor exclusiveHolder;

  public VoltronReadWriteLockActiveEntity(ClientCommunicator communicator) {
    this.communicator = communicator;
  }

  @Override
  public LockTransition invokeActive(ActiveInvokeContext<LockTransition> context, LockOperation message) {
    ClientDescriptor clientDescriptor = context.getClientDescriptor();
    return invokeActive(clientDescriptor, message);
  }

  private LockTransition invokeActive(ClientDescriptor clientDescriptor, LockOperation message) {
    switch (message.getOperation()) {
      case TRY_ACQUIRE:
        return tryAcquire(clientDescriptor, message.getHoldType());
      case ACQUIRE: return acquire(clientDescriptor, message.getHoldType());
      case RELEASE: return release(clientDescriptor, message.getHoldType());
      default: throw new AssertionError();
    }
  }

  @Override
  public void addStateTo(StateDumpCollector dump) {
    ClientDescriptor exclusiveHolder = this.exclusiveHolder;
    Set<ClientDescriptor> sharedHolders = new HashSet<>(this.sharedHolders);
    Set<ClientDescriptor> releaseListeners = new HashSet<>(this.releaseListeners);
    {
      // Dump lock holders. we dump both exclusive and shared to leave the interpretation of
      // the potential concurrency reading errors up to the person reading the state dump
      // In a normal case, there will be either exclusive OR shared holders.
      StateDumpCollector holdersDump = dump.subStateDumpCollector("holders");
      // dump the exclusive holder
      if (exclusiveHolder != null) {
        holdersDump.addState("exclusive", String.valueOf(exclusiveHolder));
      }
      // dump the shared holders.
      if(!sharedHolders.isEmpty()) {
        holdersDump.addState("shared", sharedHolders);
      }
    }
    {
      dump.addState("releaseListeners", releaseListeners);
    }
  }

  @Override
  public void connected(ClientDescriptor client) {
    //nothing to do
  }

  @Override
  public void disconnected(ClientDescriptor client) {
    releaseListeners.remove(client);
    if (client.equals(exclusiveHolder)) {
      release(client, HoldType.WRITE);
    } else if (sharedHolders.contains(client)) {
      release(client, HoldType.READ);
    }
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (clientDescriptor, bytes) -> {
      if (bytes.length == 0) {
        releaseListeners.add(clientDescriptor);
      } else {
        try {
          LockOperation message = LockMessaging.codec().decodeMessage(bytes);
          if (!invokeActive(clientDescriptor, message).isAcquired()) {
            throw new IllegalStateException("Unexpected lock acquisition failure during reconnect");
          }
        } catch (MessageCodecException ex) {
          throw new AssertionError(ex);
        }
      }
    };
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<LockOperation> syncChannel, int concurrencyKey) {
    //nothing to synchronize
  }

  private LockTransition tryAcquire(ClientDescriptor client, HoldType holdType) {
    if (exclusiveHolder != null) {
      return LockMessaging.empty();
    } else {
      switch (holdType) {
        case READ:
          sharedHolders.add(client);
          return acquired(client);
        case WRITE:
          if (sharedHolders.isEmpty()) {
            exclusiveHolder = client;
            return acquired(client);
          } else {
            return LockMessaging.empty();
          }
        default:
          throw new AssertionError();
      }
    }
  }

  private LockTransition acquire(ClientDescriptor client, HoldType holdType) {
    if (exclusiveHolder != null) {
      return waiting(client);
    } else {
      switch (holdType) {
        case READ:
          sharedHolders.add(client);
          return acquired(client);
        case WRITE:
          if (sharedHolders.isEmpty()) {
            exclusiveHolder = client;
            return acquired(client);
          } else {
            return waiting(client);
          }
        default:
          throw new AssertionError();
      }
    }
  }

  private LockTransition release(ClientDescriptor client, HoldType holdType) {
    switch (holdType) {
      case READ:
        if (sharedHolders.remove(client)) {
          if (sharedHolders.isEmpty()) {
            notifyReleaseListeners();
          }
          return LockMessaging.released();
        } else {
          return LockMessaging.empty();
        }
      case WRITE:
        if (client.equals(exclusiveHolder)) {
          exclusiveHolder = null;
          notifyReleaseListeners();
          return LockMessaging.released();
        } else {
          return LockMessaging.empty();
        }
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void createNew() {
    //nothing to do
  }

  @Override
  public void loadExisting() {
    //nothing to do
  }

  @Override
  public void destroy() {
    //nothing to do
  }

  private LockTransition acquired(ClientDescriptor client) {
    releaseListeners.remove(client);
    return LockMessaging.acquired();
  }

  private LockTransition waiting(ClientDescriptor client) {
    releaseListeners.add(client);
    return LockMessaging.empty();
  }

  private void notifyReleaseListeners() {
    for (ClientDescriptor client : releaseListeners) {
      try {
        communicator.sendNoResponse(client, LockMessaging.released());
      } catch (MessageCodecException e) {
        throw new AssertionError(e);
      }
    }
  }
}
