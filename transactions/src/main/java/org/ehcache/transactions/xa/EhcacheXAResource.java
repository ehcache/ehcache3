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
package org.ehcache.transactions.xa;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;
import org.ehcache.transactions.xa.journal.Journal;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Ludovic Orban
 */
public class EhcacheXAResource<K, V> implements XAResource {

  private static final int DEFAULT_TRANSACTION_TIMEOUT_SECONDS = 30;

  private final Store<K, SoftLock<V>> underlyingStore;
  private final Journal journal;
  private final XATransactionContextFactory<K, V> transactionContextFactory;
  private volatile Xid currentXid;
  private volatile int transactionTimeoutInSeconds = DEFAULT_TRANSACTION_TIMEOUT_SECONDS;

  public EhcacheXAResource(Store<K, SoftLock<V>> underlyingStore, Journal journal, XATransactionContextFactory<K, V> transactionContextFactory) {
    this.underlyingStore = underlyingStore;
    this.journal = journal;
    this.transactionContextFactory = transactionContextFactory;
  }

  @Override
  public void commit(Xid xid, boolean onePhase) throws XAException {
    if (currentXid != null) {
      throw new EhcacheXAException("Cannot commit a non-ended start on : " + xid, XAException.XAER_PROTO);
    }
    TransactionId transactionId = new TransactionId(xid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot commit unknown XID : " + xid, XAException.XAER_NOTA);
    }

    try {
      if (onePhase) {
        try {
          transactionContext.commitInOnePhase();
        } catch (TransactionTimeoutException tte) {
          throw new EhcacheXAException("Transaction timed out", XAException.XA_RBTIMEOUT);
        }
      } else {
        transactionContext.commit();
      }
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot commit XID : " + xid, XAException.XAER_PROTO, ise);
    } catch (CacheAccessException cae) {
      throw new EhcacheXAException("Cannot commit XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      transactionContextFactory.destroy(transactionId);
    }
  }

  @Override
  public void forget(Xid xid) throws XAException {
    TransactionId transactionId = new TransactionId(xid);
    XAState xaState = journal.getState(transactionId);
    if (xaState == null) {
      throw new EhcacheXAException("Cannot forget unknown XID : " + xid, XAException.XAER_NOTA);
    }
    if (xaState == XAState.IN_DOUBT) {
      throw new EhcacheXAException("Cannot forget in-doubt XID : " + xid, XAException.XAER_PROTO);
    }
    journal.forget(transactionId);
  }

  @Override
  public int getTransactionTimeout() throws XAException {
    return transactionTimeoutInSeconds;
  }

  @Override
  public boolean isSameRM(XAResource xaResource) throws XAException {
    return xaResource == this;
  }

  @Override
  public int prepare(Xid xid) throws XAException {
    if (currentXid != null) {
      throw new EhcacheXAException("Cannot prepare a non-ended start on : " + xid, XAException.XAER_PROTO);
    }
    TransactionId transactionId = new TransactionId(xid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot prepare unknown XID : " + xid, XAException.XAER_NOTA);
    }

    boolean readOnly = false;
    try {
      readOnly = transactionContext.prepare() == 0;
      return readOnly ? XA_RDONLY : XA_OK;
    } catch (TransactionTimeoutException tte) {
      throw new EhcacheXAException("Transaction timed out", XAException.XA_RBTIMEOUT);
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_PROTO, ise);
    } catch (CacheAccessException cae) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      if (readOnly) {
        transactionContextFactory.destroy(transactionId);
      }
    }
  }

  @Override
  public Xid[] recover(int flags) throws XAException {
    if (flags != XAResource.TMNOFLAGS && flags != XAResource.TMSTARTRSCAN && flags != XAResource.TMENDRSCAN && flags != (XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN)) {
      throw new EhcacheXAException("Recover flags not supported : " + xlat(flags), XAException.XAER_INVAL);
    }

    if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN) {
      List<Xid> xids = new ArrayList<Xid>();
      Set<TransactionId> transactionIds = journal.recover().keySet();
      for (TransactionId transactionId : transactionIds) {
        // filter-out in-flight tx
        if (!transactionContextFactory.contains(transactionId)) {
          xids.add(transactionId.getSerializableXid());
        }
      }
      return xids.toArray(new Xid[xids.size()]);
    }
    return new Xid[0];
  }

  @Override
  public void rollback(Xid xid) throws XAException {
    if (currentXid != null) {
      throw new EhcacheXAException("Cannot rollback a non-ended start on : " + xid, XAException.XAER_PROTO);
    }
    TransactionId transactionId = new TransactionId(xid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot rollback unknown XID : " + xid, XAException.XAER_NOTA);
    }

    try {
      transactionContext.rollback();
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot rollback XID : " + xid, XAException.XAER_PROTO, ise);
    } catch (CacheAccessException cae) {
      throw new EhcacheXAException("Cannot rollback XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      transactionContextFactory.destroy(transactionId);
    }
  }

  @Override
  public boolean setTransactionTimeout(int seconds) throws XAException {
    if (seconds < 0) {
      throw new EhcacheXAException("Transaction timeout must be >= 0", XAException.XAER_INVAL);
    }
    if (seconds == 0) {
      this.transactionTimeoutInSeconds = DEFAULT_TRANSACTION_TIMEOUT_SECONDS;
    } else {
      this.transactionTimeoutInSeconds = seconds;
    }
    return true;
  }

  @Override
  public void start(Xid xid, int flag) throws XAException {
    if (flag != XAResource.TMNOFLAGS && flag != XAResource.TMJOIN) {
      throw new EhcacheXAException("Start flag not supported : " + xlat(flag), XAException.XAER_INVAL);
    }
    if (currentXid != null) {
      throw new EhcacheXAException("Already started on : " + xid, XAException.XAER_PROTO);
    }

    TransactionId transactionId = new TransactionId(xid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (flag == XAResource.TMNOFLAGS) {
      if (transactionContext == null) {
        transactionContext = transactionContextFactory.createTransactionContext(transactionId, underlyingStore, journal, transactionTimeoutInSeconds);
      } else {
        throw new EhcacheXAException("Cannot start in parallel on two XIDs : " + xid, XAException.XAER_RMERR);
      }
    } else {
      if (transactionContext == null) {
        throw new EhcacheXAException("Cannot join unknown XID : " + xid, XAException.XAER_NOTA);
      }
    }

    // TODO: the timeout check currently conflicts with the resilience strategy.
    // put it back once resilience strategies are pluggable
    //if (transactionContext.hasTimedOut()) {
    //  throw new EhcacheXAException("Transaction timeout for XID : " + xid, XAException.XA_RBTIMEOUT);
    //}
    currentXid = xid;
  }

  @Override
  public void end(Xid xid, int flag) throws XAException {
    if (flag != XAResource.TMSUCCESS && flag != XAResource.TMFAIL) {
      throw new EhcacheXAException("End flag not supported : " + xlat(flag), XAException.XAER_INVAL);
    }
    if (currentXid == null) {
      throw new EhcacheXAException("Not started on : " + xid, XAException.XAER_PROTO);
    }
    TransactionId transactionId = new TransactionId(currentXid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot end unknown XID : " + xid, XAException.XAER_NOTA);
    }

    if (flag == XAResource.TMFAIL) {
      transactionContextFactory.destroy(transactionId);
    }
    currentXid = null;

    if (transactionContext.hasTimedOut()) {
      throw new EhcacheXAException("Transaction timeout for XID : " + xid, XAException.XA_RBTIMEOUT);
    }
  }

  private static String xlat(int flags) {
    StringBuilder sb = new StringBuilder();

    if ((flags & XAResource.TMSUCCESS) == XAResource.TMSUCCESS) {
      sb.append("TMSUCCESS|");
    }
    if ((flags & XAResource.TMFAIL) == XAResource.TMFAIL) {
      sb.append("TMFAIL|");
    }
    if ((flags & XAResource.TMJOIN) == XAResource.TMJOIN) {
      sb.append("TMJOIN|");
    }
    if ((flags & XAResource.TMONEPHASE) == XAResource.TMONEPHASE) {
      sb.append("TMONEPHASE|");
    }
    if ((flags & XAResource.TMSUSPEND) == XAResource.TMSUSPEND) {
      sb.append("TMRESUME|");
    }
    if ((flags & XAResource.TMRESUME) == XAResource.TMRESUME) {
      sb.append("TMRESUME|");
    }
    if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN) {
      sb.append("TMSTARTRSCAN|");
    }
    if ((flags & XAResource.TMENDRSCAN) == XAResource.TMENDRSCAN) {
      sb.append("TMENDRSCAN|");
    }

    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    } else {
      sb.append("TMNOFLAGS");
    }

    return sb.toString();
  }

  public XATransactionContext<K, V> getCurrentContext() throws CacheAccessException {
    if (currentXid == null) {
      return null;
    }
    return transactionContextFactory.get(new TransactionId(currentXid));
  }
}
