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

package org.ehcache.transactions.xa.internal;

import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.transactions.xa.EhcacheXAException;
import org.ehcache.transactions.xa.internal.journal.Journal;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Ehcache {@link XAResource} implementation.
 *
 * @author Ludovic Orban
 */
public class EhcacheXAResource<K, V> implements XAResource {

  private static final int DEFAULT_TRANSACTION_TIMEOUT_SECONDS = 30;

  private final Store<K, SoftLock<V>> underlyingStore;
  private final Journal<K> journal;
  private final XATransactionContextFactory<K, V> transactionContextFactory;
  private volatile Xid currentXid;
  private volatile int transactionTimeoutInSeconds = DEFAULT_TRANSACTION_TIMEOUT_SECONDS;

  public EhcacheXAResource(Store<K, SoftLock<V>> underlyingStore, Journal<K> journal, XATransactionContextFactory<K, V> transactionContextFactory) {
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

    try {
      if (onePhase) {
        if (transactionContext == null) {
          throw new EhcacheXAException("Cannot commit in one phase unknown XID : " + xid, XAException.XAER_NOTA);
        }
        try {
          transactionContext.commitInOnePhase();
        } catch (XATransactionContext.TransactionTimeoutException tte) {
          throw new EhcacheXAException("Transaction timed out", XAException.XA_RBTIMEOUT);
        }
      } else {
        XATransactionContext<K, V> commitContext = transactionContext;
        if (commitContext == null) {
          // recovery commit
          commitContext = new XATransactionContext<K, V>(new TransactionId(new SerializableXid(xid)), underlyingStore, journal, null, 0L);
        }
        commitContext.commit(transactionContext == null);
      }
    } catch (IllegalArgumentException iae) {
      throw new EhcacheXAException("Cannot commit unknown XID : " + xid, XAException.XAER_NOTA);
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot commit XID : " + xid, XAException.XAER_PROTO, ise);
    } catch (StoreAccessException cae) {
      throw new EhcacheXAException("Cannot commit XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      if (transactionContext != null) {
        transactionContextFactory.destroy(transactionId);
      }
    }
  }

  @Override
  public void forget(Xid xid) throws XAException {
    TransactionId transactionId = new TransactionId(xid);
    if (journal.isInDoubt(transactionId)) {
      throw new EhcacheXAException("Cannot forget in-doubt XID : " + xid, XAException.XAER_PROTO);
    }
    if (!journal.isHeuristicallyTerminated(transactionId)) {
      throw new EhcacheXAException("Cannot forget unknown XID : " + xid, XAException.XAER_NOTA);
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

    boolean destroyContext = false;
    try {
      destroyContext = transactionContext.prepare() == 0;
      return destroyContext ? XA_RDONLY : XA_OK;
    } catch (XATransactionContext.TransactionTimeoutException tte) {
      destroyContext = true;
      throw new EhcacheXAException("Transaction timed out", XAException.XA_RBTIMEOUT);
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_PROTO, ise);
    } catch (StoreAccessException cae) {
      throw new EhcacheXAException("Cannot prepare XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      if (destroyContext) {
        transactionContextFactory.destroy(transactionId);
      }
    }
  }

  @Override
  public Xid[] recover(int flags) throws XAException {
    if (flags != XAResource.TMNOFLAGS && flags != XAResource.TMSTARTRSCAN && flags != XAResource.TMENDRSCAN && flags != (XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN)) {
      throw new EhcacheXAException("Recover flags not supported : " + xaResourceFlagsToString(flags), XAException.XAER_INVAL);
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

    try {
      XATransactionContext<K, V> rollbackContext = transactionContext;
      if (rollbackContext == null) {
        // recovery rollback
        rollbackContext = new XATransactionContext<K, V>(new TransactionId(new SerializableXid(xid)), underlyingStore, journal, null, 0L);
      }

      rollbackContext.rollback(transactionContext == null);
    } catch (IllegalStateException ise) {
      throw new EhcacheXAException("Cannot rollback unknown XID : " + xid, XAException.XAER_NOTA);
    } catch (StoreAccessException cae) {
      throw new EhcacheXAException("Cannot rollback XID : " + xid, XAException.XAER_RMERR, cae);
    } finally {
      if (transactionContext != null) {
        transactionContextFactory.destroy(transactionId);
      }
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
      throw new EhcacheXAException("Start flag not supported : " + xaResourceFlagsToString(flag), XAException.XAER_INVAL);
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
        throw new EhcacheXAException("Cannot start in parallel on two XIDs : starting " + xid, XAException.XAER_RMERR);
      }
    } else {
      if (transactionContext == null) {
        throw new EhcacheXAException("Cannot join unknown XID : " + xid, XAException.XAER_NOTA);
      }
    }

    if (transactionContext.hasTimedOut()) {
      transactionContextFactory.destroy(transactionId);
      throw new EhcacheXAException("Transaction timeout for XID : " + xid, XAException.XA_RBTIMEOUT);
    }
    currentXid = xid;
  }

  @Override
  public void end(Xid xid, int flag) throws XAException {
    if (flag != XAResource.TMSUCCESS && flag != XAResource.TMFAIL) {
      throw new EhcacheXAException("End flag not supported : " + xaResourceFlagsToString(flag), XAException.XAER_INVAL);
    }
    if (currentXid == null) {
      throw new EhcacheXAException("Not started on : " + xid, XAException.XAER_PROTO);
    }
    TransactionId transactionId = new TransactionId(currentXid);
    XATransactionContext<K, V> transactionContext = transactionContextFactory.get(transactionId);
    if (transactionContext == null) {
      throw new EhcacheXAException("Cannot end unknown XID : " + xid, XAException.XAER_NOTA);
    }

    boolean destroyContext = false;
    if (flag == XAResource.TMFAIL) {
      destroyContext = true;
    }
    currentXid = null;

    try {
      if (transactionContext.hasTimedOut()) {
        destroyContext = true;
        throw new EhcacheXAException("Transaction timeout for XID : " + xid, XAException.XA_RBTIMEOUT);
      }
    } finally {
      if (destroyContext) {
        transactionContextFactory.destroy(transactionId);
      }
    }
  }

  public XATransactionContext<K, V> getCurrentContext() {
    if (currentXid == null) {
      return null;
    }
    return transactionContextFactory.get(new TransactionId(currentXid));
  }

  private static String xaResourceFlagsToString(int flags) {
    StringBuilder sb = new StringBuilder();

    Set<Map.Entry<Integer, String>> entries = XARESOURCE_FLAGS_TO_NAMES.entrySet();
    for (Map.Entry<Integer, String> entry : entries) {
      int constant = entry.getKey();
      String name = entry.getValue();
      if (constant != 0 && (flags & constant) == constant) {
        sb.append(name).append("|");
      }
    }

    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    } else {
      sb.append(XARESOURCE_FLAGS_TO_NAMES.get(0));
    }

    return sb.toString();
  }

  private static final Map<Integer, String> XARESOURCE_FLAGS_TO_NAMES = new HashMap<Integer, String>();
  static {
    try {
      for (Field field : XAResource.class.getFields()) {
        String name = field.getName();
        if (field.getType().equals(int.class) && name.startsWith("TM")) {
          Integer contant = (Integer) field.get(XAResource.class);
          XARESOURCE_FLAGS_TO_NAMES.put(contant, name);
        }
      }
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Cannot initialize XAResource flags map", iae);
    }
  }

}
