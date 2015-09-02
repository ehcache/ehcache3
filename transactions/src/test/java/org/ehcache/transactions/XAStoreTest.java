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

import org.ehcache.Cache;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.BiFunction;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.internal.store.heap.OnHeapStore;
import org.ehcache.internal.store.offheap.MemorySizeParser;
import org.ehcache.internal.store.offheap.OffHeapStore;
import org.ehcache.internal.store.offheap.OffHeapStoreLifecycleHelper;
import org.ehcache.internal.store.tiering.CacheStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author Ludovic Orban
 */
public class XAStoreTest {

  private final TestTransactionManager testTransactionManager = new TestTransactionManager();

  @Test
  public void testSimpleGetPutRemove() throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null, null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(), keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, true);
    XaTransactionStateStore stateStore = new TransientXaTransactionStateStore();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(onHeapStore, testTransactionManager, testTimeSource, stateStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);

      xaStore.put(1L, "one");

      valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.remove(1L);
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "one");

    testTransactionManager.begin();
    {
      xaStore.put(1L, "un");
      Store.ValueHolder<String> valueHolder = xaStore.get(1L);
      System.out.println(valueHolder);
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");
  }

  private void assertMapping(XAStore<Long, String> xaStore, long key, String value) throws Exception {
    testTransactionManager.begin();
    {
      Store.ValueHolder<String> valueHolder = xaStore.get(key);
      if (value != null) {
        assertThat(valueHolder.value(), equalTo(value));
      } else {
        assertThat(valueHolder, is(nullValue()));
      }
    }
    testTransactionManager.commit();
  }


  @Test
  public void testIterate() throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null, null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(), keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, true);
    XaTransactionStateStore stateStore = new TransientXaTransactionStateStore();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(onHeapStore, testTransactionManager, testTimeSource, stateStore);

    testTransactionManager.begin();
    {
      xaStore.put(1L, "one");
      xaStore.put(2L, "two");
      xaStore.put(3L, "three");
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      xaStore.put(0L, "zero");
      xaStore.put(1L, "un");
      xaStore.put(2L, "two");
      xaStore.remove(3L);

      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        System.out.println(next.getKey() + " : " + next.getValue().value());
      }
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      Store.Iterator<Cache.Entry<Long, Store.ValueHolder<String>>> iterator = xaStore.iterator();
      while (iterator.hasNext()) {
        Cache.Entry<Long, Store.ValueHolder<String>> next = iterator.next();
        System.out.println(next.getKey() + " : " + next.getValue().value());
      }
    }
    testTransactionManager.commit();


  }

  @Test
  public void testCompute() throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    Serializer<Long> keySerializer = new JavaSerializer<Long>(classLoader);
    Serializer<SoftLock> valueSerializer = new JavaSerializer<SoftLock>(classLoader);
    Store.Configuration<Long, SoftLock> onHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null, null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build(), keySerializer, valueSerializer);
    TestTimeSource testTimeSource = new TestTimeSource();
    OnHeapStore<Long, SoftLock<String>> onHeapStore = (OnHeapStore) new OnHeapStore<Long, SoftLock>(onHeapConfig, testTimeSource, true);
    Store.Configuration<Long, SoftLock> offHeapConfig = new StoreConfigurationImpl<Long, SoftLock>(Long.class, SoftLock.class, null, null, classLoader, Expirations.noExpiration(), ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(10, MemoryUnit.MB).build(), keySerializer, valueSerializer);
    OffHeapStore<Long, SoftLock<String>> offHeapStore = (OffHeapStore) new OffHeapStore<Long, SoftLock>(offHeapConfig, testTimeSource, MemorySizeParser.parse("10M"));
    OffHeapStoreLifecycleHelper.init(offHeapStore);
    CacheStore<Long, SoftLock<String>> cacheStore = new CacheStore<Long, SoftLock<String>>(onHeapStore, offHeapStore);


    XaTransactionStateStore stateStore = new TransientXaTransactionStateStore();

    XAStore<Long, String> xaStore = new XAStore<Long, String>(cacheStore, testTransactionManager, testTimeSource, stateStore);

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed1 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return "one";
        }
      });
      System.out.println("computed : " + computed1);
      Store.ValueHolder<String> computed2 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return "un";
        }
      });
      System.out.println("computed : " + computed2);
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "un");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed3 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return "eins";
        }
      });
      System.out.println("computed : " + computed3);
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed3 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return null;
        }
      });
      System.out.println("computed : " + computed3);
    }
    testTransactionManager.rollback();

    assertMapping(xaStore, 1L, "eins");

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed3 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return null;
        }
      });
      System.out.println("computed : " + computed3);
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      Store.ValueHolder<String> computed3 = xaStore.compute(1L, new BiFunction<Long, String, String>() {
        @Override
        public String apply(Long aLong, String s) {
          System.out.println("computing : " + s);
          return null;
        }
      });
      System.out.println("computed : " + computed3);
    }
    testTransactionManager.commit();

    testTransactionManager.begin();
    {
      boolean b = xaStore.containsKey(1L);
      System.out.println("containsKey: " + b);
      assertThat(b, is(false));
      xaStore.put(1L, "uno");
      b = xaStore.containsKey(1L);
      System.out.println("containsKey: " + b);
      assertThat(b, is(true));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, "uno");

    testTransactionManager.begin();
    {
      boolean b = xaStore.containsKey(1L);
      System.out.println("containsKey: " + b);
      assertThat(b, is(true));
      xaStore.remove(1L);
      b = xaStore.containsKey(1L);
      System.out.println("containsKey: " + b);
      assertThat(b, is(false));
    }
    testTransactionManager.commit();

    assertMapping(xaStore, 1L, null);

    OffHeapStoreLifecycleHelper.close(offHeapStore);
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
      currentTransaction.rollback();
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
    final List<Synchronization> synchronizations = new CopyOnWriteArrayList<Synchronization>();

    public TestTransaction(long gtrid) {
      this.gtrid = gtrid;
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
      try {
        Set<Map.Entry<XAResource, TestXid>> entries = xids.entrySet();

        // delist
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().end(entry.getValue(), XAResource.TMNOFLAGS);
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }

        fireBeforeCompletion();

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
      } finally {
        fireAfterCompletion(Status.STATUS_COMMITTED);
      }
    }

    @Override
    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
      return true;
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
      synchronizations.add(sync);
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
      try {
        Set<Map.Entry<XAResource, TestXid>> entries = xids.entrySet();

        // delist
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().end(entry.getValue(), XAResource.TMNOFLAGS);
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }

        // rollback
        for (Map.Entry<XAResource, TestXid> entry : entries) {
          try {
            entry.getKey().rollback(entry.getValue());
          } catch (XAException e) {
            throw (SystemException) new SystemException(XAException.XAER_RMERR).initCause(e);
          }
        }
      } finally {
        fireAfterCompletion(Status.STATUS_ROLLEDBACK);
      }
    }

    private void fireBeforeCompletion() {
      for (Synchronization synchronization : synchronizations) {
        synchronization.beforeCompletion();
      }
    }

    private void fireAfterCompletion(int status) {
      for (Synchronization synchronization : synchronizations) {
        synchronization.afterCompletion(status);
      }
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
        return formatId == otherXid.formatId &&
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
      for (int i = uid.length - 1; i > 0; i--) {
        hash <<= 1;

        if (hash < 0) {
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
