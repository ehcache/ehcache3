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

import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Ludovic Orban
 */
public class XAStoreTest {

  @Test
  public void testSimple() throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null, null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build());
    TestTimeSource testTimeSource = new TestTimeSource();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, true, keySerializer, valueSerializer);
    TestTransactionManager testTransactionManager = new TestTransactionManager();
    XaTransactionStateStore stateStore = new TransientXaTransactionStateStore();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(onHeapStore, testTransactionManager, testTimeSource, stateStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }

    xaStore.put(1L, "one");

    {
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }
    testTransactionManager.commit();

  }


  static class TestTransactionManager implements TransactionManager {

    volatile TestTransaction currentTransaction;
    final AtomicLong gtridGenerator = new AtomicLong();

    @Override
    public void begin() throws NotSupportedException, SystemException {
      currentTransaction = new TestTransaction(gtridGenerator.incrementAndGet());
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      currentTransaction.commit();
      currentTransaction = null;
    }

    @Override
    public int getStatus() throws SystemException {
      return 0;
    }

    @Override
    public Transaction getTransaction() throws SystemException {
      return currentTransaction;
    }

    @Override
    public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
      currentTransaction = null;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {

    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {

    }

    @Override
    public Transaction suspend() throws SystemException {
      return null;
    }
  }

  static class TestTransaction implements Transaction {

    final long gtrid;
    final Map<XAResource, TestXid> xids = new IdentityHashMap<XAResource, TestXid>();
    final AtomicLong bqualGenerator = new AtomicLong();

    public TestTransaction(long gtrid) {
      this.gtrid = gtrid;
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      Set<Map.Entry<XAResource, TestXid>> entries = xids.entrySet();

      // delist
      for (Map.Entry<XAResource, TestXid> entry : entries) {
        try {
          entry.getKey().end(entry.getValue(), XAResource.TMNOFLAGS);
        } catch (XAException e) {
          throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
        }
      }

      // prepare
      for (Map.Entry<XAResource, TestXid> entry : entries) {
        try {
          entry.getKey().prepare(entry.getValue());
        } catch (XAException e) {
          throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
        }
      }

      // commit
      for (Map.Entry<XAResource, TestXid> entry : entries) {
        try {
          entry.getKey().commit(entry.getValue(), false);
        } catch (XAException e) {
          throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
        }
      }
    }

    @Override
    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
      return false;
    }

    @Override
    public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException, SystemException {
      TestXid testXid = xids.get(xaRes);
      if (testXid == null) {
        testXid = new TestXid(gtrid, bqualGenerator.incrementAndGet());
        xids.put(xaRes, testXid);
      }

      try {
        xaRes.start(testXid, XAResource.TMNOFLAGS);
      } catch (XAException e) {
        throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
      }
      return true;
    }

    @Override
    public int getStatus() throws SystemException {
      return 0;
    }

    @Override
    public void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException, SystemException {

    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {

    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {

    }
  }

  static class TestXid implements Xid {

    private int formatId = 123456;
    private byte[] gtrid;
    private byte[] bqual;

    public TestXid(long gtrid, long bqual) {
      this.gtrid = longToBytes(gtrid);
      this.bqual = longToBytes(bqual);
    }

    public TestXid(Xid xid) {
      this.formatId = xid.getFormatId();
      this.gtrid = xid.getGlobalTransactionId();
      this.bqual = xid.getBranchQualifier();
    }

    public int getFormatId() {
      return formatId;
    }

    public byte[] getGlobalTransactionId() {
      return gtrid;
    }

    public byte[] getBranchQualifier() {
      return bqual;
    }

    @Override
    public int hashCode() {
      return formatId + arrayHashCode(gtrid) + arrayHashCode(bqual);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TestXid) {
        TestXid otherXid = (TestXid) o;
        return  formatId == otherXid.formatId &&
            Arrays.equals(gtrid, otherXid.gtrid) &&
            Arrays.equals(bqual, otherXid.bqual);
      }
      return false;
    }

    @Override
    public String toString() {
      return "TestXid [" + hashCode() + "]";
    }

    private static int arrayHashCode(byte[] uid) {
      int hash = 0;
      for (int i = uid.length -1; i > 0 ;i--) {
        hash <<= 1;

        if ( hash < 0 ) {
          hash |= 1;
        }

        hash ^= uid[i];
      }
      return hash;
    }

    public static byte[] longToBytes(long aLong) {
      byte[] array = new byte[8];

      array[7] = (byte) (aLong & 0xff);
      array[6] = (byte) ((aLong >> 8) & 0xff);
      array[5] = (byte) ((aLong >> 16) & 0xff);
      array[4] = (byte) ((aLong >> 24) & 0xff);
      array[3] = (byte) ((aLong >> 32) & 0xff);
      array[2] = (byte) ((aLong >> 40) & 0xff);
      array[1] = (byte) ((aLong >> 48) & 0xff);
      array[0] = (byte) ((aLong >> 56) & 0xff);

      return array;
    }

  }


}
