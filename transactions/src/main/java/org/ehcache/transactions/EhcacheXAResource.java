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
package org.ehcache.transactions;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.cache.Store;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class EhcacheXAResource<K, V> implements XAResource {

  private final Map<Xid, XATransactionContext<K, V>> transactionContexts = new ConcurrentHashMap<Xid, XATransactionContext<K, V>>();
  private final Store<K, SoftLock<K, V>> underlyingStore;
  private volatile Xid currentXid;

  public EhcacheXAResource(Store<K, SoftLock<K, V>> underlyingStore) {
    this.underlyingStore = underlyingStore;
  }

  @Override
  public void commit(Xid xid, boolean onePhase) throws XAException {
    XATransactionContext<K, V> transactionContext = transactionContexts.get(xid);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot commit unknown XID : " + xid, XAException.XAER_PROTO);
    }

    try {
      transactionContext.commit();
    } catch (CacheAccessException cae) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_RMERR, cae);
    }
  }

  @Override
  public void end(Xid xid, int flag) throws XAException {
    if (flag != XAResource.TMNOFLAGS) {
      throw new EhcacheXAException("End flag not supported : " + xlat(flag), XAException.XAER_PROTO);
    }
    if (currentXid == null) {
      throw new EhcacheXAException("Not started on : " + xid, XAException.XAER_PROTO);
    }

    currentXid = null;
  }

  @Override
  public void forget(Xid xid) throws XAException {

  }

  @Override
  public int getTransactionTimeout() throws XAException {
    return 0;
  }

  @Override
  public boolean isSameRM(XAResource xaResource) throws XAException {
    return false;
  }

  @Override
  public int prepare(Xid xid) throws XAException {
    XATransactionContext<K, V> transactionContext = transactionContexts.get(xid);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot prepare unknown XID : " + xid, XAException.XAER_PROTO);
    }

    try {
      return (transactionContext.prepare() > 0) ? XA_OK : XA_RDONLY;
    } catch (CacheAccessException cae) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_RMERR, cae);
    }
  }

  @Override
  public Xid[] recover(int i) throws XAException {
    return new Xid[0];
  }

  @Override
  public void rollback(Xid xid) throws XAException {

  }

  @Override
  public boolean setTransactionTimeout(int i) throws XAException {
    return false;
  }

  @Override
  public void start(Xid xid, int flag) throws XAException {
    if (flag != XAResource.TMNOFLAGS) {
      throw new EhcacheXAException("Start flag not supported : " + xlat(flag), XAException.XAER_PROTO);
    }
    if (currentXid != null) {
      throw new EhcacheXAException("Already started on : " + xid, XAException.XAER_PROTO);
    }

    currentXid = xid;
    if (!transactionContexts.containsKey(xid)) {
      transactionContexts.put(xid, new XATransactionContext<K, V>(underlyingStore));
    }
  }

  private static String xlat(int flag) {
    return "" + flag;
  }

  public XATransactionContext<K, V> getCurrentContext() throws CacheAccessException {
    if (currentXid == null) {
      throw new CacheAccessException("Cannot get current context when XAResource.start() was not called");
    }
    return transactionContexts.get(currentXid);
  }
}
