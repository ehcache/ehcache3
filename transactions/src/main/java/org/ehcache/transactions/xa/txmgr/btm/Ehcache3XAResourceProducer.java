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
package org.ehcache.transactions.xa.txmgr.btm;

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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.transaction.xa.XAResource;

/**
 * @author Ludovic Orban
 */
class Ehcache3XAResourceProducer extends ResourceBean implements XAResourceProducer {

  private static final long serialVersionUID = -6421881731950504009L;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<XAResource, Ehcache3XAResourceHolder> xaResourceHolders = new ConcurrentHashMap<>();
  private volatile transient RecoveryXAResourceHolder recoveryXAResourceHolder;

  Ehcache3XAResourceProducer() {
    setApplyTransactionTimeout(true);
  }

  void addXAResource(XAResource xaResource) {
    Ehcache3XAResourceHolder xaResourceHolder = new Ehcache3XAResourceHolder(xaResource, this);
    xaResourceHolders.put(xaResource, xaResourceHolder);
  }

  boolean removeXAResource(XAResource xaResource) {
    return xaResourceHolders.remove(xaResource) != null;
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

  boolean isEmpty() {
    return xaResourceHolders.isEmpty();
  }

  public void endRecovery() {
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
  public XAStatefulHolder createPooledConnection(Object xaFactory, ResourceBean bean) {
    throw new UnsupportedOperationException("Ehcache is not connection-oriented");
  }

  @Override
  public Reference getReference() {
    return new Reference(Ehcache3XAResourceProducer.class.getName(),
        new StringRefAddr("uniqueName", getUniqueName()),
        ResourceObjectFactory.class.getName(), null);
  }

  public String toString() {
    return "a Ehcache3XAResourceProducer with uniqueName " + getUniqueName();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    xaResourceHolders.clear();
    out.defaultWriteObject();
  }

}
