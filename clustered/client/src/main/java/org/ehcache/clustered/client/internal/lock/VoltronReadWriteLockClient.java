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

package org.ehcache.clustered.client.internal.lock;

import java.util.concurrent.Semaphore;
import org.ehcache.clustered.common.internal.lock.LockMessaging;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockOperation;
import org.ehcache.clustered.common.internal.lock.LockMessaging.LockTransition;
import org.ehcache.clustered.common.internal.lock.LockMessaging.HoldType;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

public class VoltronReadWriteLockClient implements Entity {

  private final EntityClientEndpoint<LockOperation, LockTransition> endpoint;

  private final Semaphore wakeup = new Semaphore(0);

  private volatile LockOperation currentState = null;

  public VoltronReadWriteLockClient(EntityClientEndpoint<LockOperation, LockTransition> endpoint) {
    this.endpoint = endpoint;
    this.endpoint.setDelegate(new EndpointDelegate<LockTransition>() {
      @Override
      public void handleMessage(LockTransition response) {
        if (response.isReleased()) {
          wakeup.release();
        }
      }

      @Override
      public byte[] createExtendedReconnectData() {
        try {
          LockOperation state = getCurrentState();
          if (state == null) {
            return new byte[0];
          } else {
            return LockMessaging.codec().encodeMessage(state);
          }
        } catch (MessageCodecException e) {
          throw new AssertionError(e);
        }
      }

      @Override
      public void didDisconnectUnexpectedly() { }
    });
  }

  @Override
  public void close() {
    endpoint.close();
  }

  public boolean tryLock(HoldType type) {
    LockTransition transition = invoke(LockMessaging.tryLock(type));
    if (transition.isAcquired()) {
      currentState = LockMessaging.lock(type);
      return true;
    } else {
      return false;
    }
  }

  public void lock(HoldType type) {
    while (true) {
      LockTransition transition = invoke(LockMessaging.lock(type));
      if (transition.isAcquired()) {
        currentState = LockMessaging.lock(type);
        return;
      } else {
        wakeup.acquireUninterruptibly();
      }
    }
  }

  public void unlock(HoldType type) {
    LockTransition transition = invoke(LockMessaging.unlock(type));
    if (transition.isReleased()) {
      currentState = null;
    } else {
      throw new IllegalMonitorStateException();
    }
  }

  private LockOperation getCurrentState() {
    return currentState;
  }

  private LockTransition invoke(LockOperation operation) {
    try {
      InvokeFuture<LockTransition> result = endpoint.beginInvoke().message(operation).replicate(false).invoke();
      boolean interrupted = false;
      try {
        while (true) {
          try {
            return result.get();
          } catch (InterruptedException ex) {
            interrupted = true;
          } catch (EntityException ex) {
            throw new IllegalStateException(ex);
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } catch (MessageCodecException ex) {
      throw new AssertionError(ex);
    }
  }
}
