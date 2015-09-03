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
package org.ehcache.transactions.txmgrs.btm;

import bitronix.tm.internal.BitronixRuntimeException;
import bitronix.tm.internal.XAResourceHolderState;
import bitronix.tm.recovery.RecoveryException;
import bitronix.tm.resource.ResourceObjectFactory;
import bitronix.tm.resource.ResourceRegistrar;
import bitronix.tm.resource.common.RecoveryXAResourceHolder;
import bitronix.tm.resource.common.ResourceBean;
import bitronix.tm.resource.common.XAResourceHolder;
import bitronix.tm.resource.common.XAResourceProducer;
import bitronix.tm.resource.common.XAStatefulHolder;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Ludovic Orban
 */
public class Ehcache3XAResourceProducer extends ResourceBean implements XAResourceProducer {


  private static final ConcurrentMap<String, Ehcache3XAResourceProducer> producers = new ConcurrentHashMap<String, Ehcache3XAResourceProducer>();

  private final Map<XAResource, XAResourceHolder> xaResourceHolders = new ConcurrentIdentityHashMap<XAResource, XAResourceHolder>();

  private volatile RecoveryXAResourceHolder recoveryXAResourceHolder;



  /**
   * Register an XAResource of a cache with BTM. The first time a XAResource is registered a new
   * EhCacheXAResourceProducer is created to hold it.
   * @param uniqueName the uniqueName of this XAResourceProducer, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  public static void registerXAResource(String uniqueName, XAResource xaResource) {
    Ehcache3XAResourceProducer xaResourceProducer = producers.get(uniqueName);

    if (xaResourceProducer == null) {
      xaResourceProducer = new Ehcache3XAResourceProducer();
      xaResourceProducer.setUniqueName(uniqueName);
      // the initial xaResource must be added before init() can be called
      xaResourceProducer.addXAResource(xaResource);

      Ehcache3XAResourceProducer previous = producers.putIfAbsent(uniqueName, xaResourceProducer);
      if (previous == null) {
        xaResourceProducer.init();
      } else {
        previous.addXAResource(xaResource);
      }
    } else {
      xaResourceProducer.addXAResource(xaResource);
    }
  }

  /**
   * Unregister an XAResource of a cache from BTM.
   * @param uniqueName the uniqueName of this XAResourceProducer, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  public static void unregisterXAResource(String uniqueName, XAResource xaResource) {
    Ehcache3XAResourceProducer xaResourceProducer = producers.get(uniqueName);

    if (xaResourceProducer != null) {
      boolean found = xaResourceProducer.removeXAResource(xaResource);
      if (!found) {
        throw new IllegalStateException("no XAResource " + xaResource + " found in XAResourceProducer with name " + uniqueName);
      }
      if (xaResourceProducer.xaResourceHolders.isEmpty()) {
        xaResourceProducer.close();
        producers.remove(uniqueName);
      }
    } else {
      throw new IllegalStateException("no XAResourceProducer registered with name " + uniqueName);
    }
  }


  private void addXAResource(XAResource xaResource) {
    Ehcache3XAResourceHolder xaResourceHolder = new Ehcache3XAResourceHolder(xaResource, this);
    xaResourceHolders.put(xaResource, xaResourceHolder);
  }

  private boolean removeXAResource(XAResource xaResource) {
    return xaResourceHolders.remove(xaResource) != null;
  }



  public Ehcache3XAResourceProducer() {
    setApplyTransactionTimeout(true);
  }

  public XAResourceHolderState startRecovery() throws RecoveryException {
    if (recoveryXAResourceHolder != null) {
      throw new RecoveryException("recovery already in progress on " + this);
    }

    if (xaResourceHolders.isEmpty()) {
      throw new RecoveryException("no XAResource registered, recovery cannot be done on " + this);
    }

    recoveryXAResourceHolder = new RecoveryXAResourceHolder(xaResourceHolders.values().iterator().next());
    return new XAResourceHolderState(recoveryXAResourceHolder, this);
  }

  public void endRecovery() throws RecoveryException {
    recoveryXAResourceHolder = null;
  }


  @Override
  public void setFailed(boolean failed) {
    // cache cannot fail as it's not connection oriented
  }

  @Override
  public XAResourceHolder findXAResourceHolder(XAResource xaResource) {
    return xaResourceHolders.get(xaResource);
  }

  @Override
  public void init() {
    try {
      ResourceRegistrar.register(this);
    } catch (RecoveryException ex) {
      throw new BitronixRuntimeException("error recovering " + this, ex);
    }
  }

  @Override
  public void close() {
    xaResourceHolders.clear();
    ResourceRegistrar.unregister(this);
  }

  @Override
  public XAStatefulHolder createPooledConnection(Object xaFactory, ResourceBean bean) throws Exception {
    throw new UnsupportedOperationException("Ehcache is not connection-oriented");
  }

  @Override
  public Reference getReference() throws NamingException {
    return new Reference(Ehcache3XAResourceProducer.class.getName(),
        new StringRefAddr("uniqueName", getUniqueName()),
        ResourceObjectFactory.class.getName(), null);
  }

  public String toString() {
    return "a Ehcache3XAResourceProducer with uniqueName " + getUniqueName();
  }

}
