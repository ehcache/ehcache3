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

import org.ehcache.internal.TestTimeSource;
import org.ehcache.impl.internal.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.transactions.xa.internal.commands.StoreEvictCommand;
import org.ehcache.transactions.xa.internal.commands.StorePutCommand;
import org.ehcache.transactions.xa.internal.commands.StoreRemoveCommand;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.transactions.xa.utils.TestXid;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class XATransactionContextTest {

  @Test
  public void testSimpleCommands() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), null, null, timeSource, timeSource.getTimeMillis() + 30000);

    assertThat(xaTransactionContext.touched(1L), is(false));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("old", new XAValueHolder<String>("new", timeSource.getTimeMillis())));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(true));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L).value(), equalTo("new"));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), equalTo("new"));

    xaTransactionContext.addCommand(1L, new StoreRemoveCommand<String>("old"));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(true));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));

    xaTransactionContext.addCommand(1L, new StoreEvictCommand<String>("old"));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(true));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));
  }

  @Test
  public void testCommandsOverrideEachOther() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), null, null, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("old", new XAValueHolder<String>("new", timeSource.getTimeMillis())));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(true));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L).value(), equalTo("new"));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), equalTo("new"));

    xaTransactionContext.addCommand(1L, new StoreRemoveCommand<String>("old"));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(true));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));

    xaTransactionContext.addCommand(1L, new StoreRemoveCommand<String>("old2"));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(true));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old2"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("old2", new XAValueHolder<String>("new2", timeSource.getTimeMillis())));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(true));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L).value(), equalTo("new2"));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old2"));
    assertThat(xaTransactionContext.newValueOf(1L), equalTo("new2"));
  }

  @Test
  public void testEvictCommandCannotBeOverridden() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), null, null, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("old", new XAValueHolder<String>("new", timeSource.getTimeMillis())));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(true));
    assertThat(xaTransactionContext.evicted(1L), is(false));
    assertThat(xaTransactionContext.newValueHolderOf(1L).value(), equalTo("new"));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), equalTo("new"));

    xaTransactionContext.addCommand(1L, new StoreEvictCommand<String>("old"));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(true));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("old2", new XAValueHolder<String>("new2", timeSource.getTimeMillis())));
    assertThat(xaTransactionContext.touched(1L), is(true));
    assertThat(xaTransactionContext.removed(1L), is(false));
    assertThat(xaTransactionContext.updated(1L), is(false));
    assertThat(xaTransactionContext.evicted(1L), is(true));
    assertThat(xaTransactionContext.newValueHolderOf(1L), is(nullValue()));
    assertThat(xaTransactionContext.oldValueOf(1L), equalTo("old"));
    assertThat(xaTransactionContext.newValueOf(1L), is(nullValue()));
  }

  @Test
  public void testHasTimedOut() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), null, null, timeSource, timeSource.getTimeMillis() + 30000);

    assertThat(xaTransactionContext.hasTimedOut(), is(false));
    timeSource.advanceTime(30000);
    assertThat(xaTransactionContext.hasTimedOut(), is(true));
  }

  @Test
  public void testPrepareReadOnly() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    assertThat(xaTransactionContext.prepare(), is(0));

    verify(journal, times(1)).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), eq(Collections.<Long>emptySet()));
    verify(journal, times(0)).saveCommitted(eq(new TransactionId(new TestXid(0, 0))), anyBoolean());
    verify(journal, times(1)).saveRolledBack(eq(new TransactionId(new TestXid(0, 0))), eq(false));
  }

  @Test
  public void testPrepare() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>(null, new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));
    xaTransactionContext.addCommand(3L, new StoreEvictCommand<String>("three"));

    Store.ValueHolder<SoftLock<String>> mockValueHolder = mock(Store.ValueHolder.class);
    when(mockValueHolder.value()).thenReturn(new SoftLock<String>(null, "two", null));
    when(underlyingStore.get(eq(2L))).thenReturn(mockValueHolder);
    when(underlyingStore.replace(eq(2L), eq(new SoftLock<String>(null, "two", null)), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)))).thenReturn(ReplaceStatus.HIT);

    final AtomicReference<Collection<Long>> savedInDoubt = new AtomicReference<Collection<Long>>();
    // doAnswer is required to make a copy of the keys collection because xaTransactionContext.prepare() clears it before the verify(journal, times(1)).saveInDoubt(...) assertion can be made.
    // See: http://stackoverflow.com/questions/17027368/mockito-what-if-argument-passed-to-mock-is-modified
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Collection<Long> o = (Collection<Long>) invocation.getArguments()[1];
        savedInDoubt.set(new HashSet<Long>(o));
        return null;
      }
    }).when(journal).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), anyCollection());

    assertThat(xaTransactionContext.prepare(), is(3));

    Assert.assertThat(savedInDoubt.get(), containsInAnyOrder(1L, 2L, 3L));

    verify(journal, times(1)).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), anyCollection());
    verify(journal, times(0)).saveCommitted(eq(new TransactionId(new TestXid(0, 0))), anyBoolean());
    verify(journal, times(0)).saveRolledBack(eq(new TransactionId(new TestXid(0, 0))), anyBoolean());

    verify(underlyingStore, times(0)).get(1L);
    verify(underlyingStore, times(1)).putIfAbsent(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), null, new XAValueHolder<String>("un", timeSource.getTimeMillis()))));
    verify(underlyingStore, times(0)).get(2L);
    verify(underlyingStore, times(1)).replace(eq(2L), eq(new SoftLock<String>(null, "two", null)), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)));
    verify(underlyingStore, times(0)).get(3L);
    verify(underlyingStore, times(1)).remove(eq(3L));
  }

  @Test
  public void testCommitNotPreparedInFlightThrows() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StorePutCommand<String>("two", new XAValueHolder<String>("deux", timeSource.getTimeMillis())));

    Store.ValueHolder<SoftLock<String>> mockValueHolder = mock(Store.ValueHolder.class);
    when(mockValueHolder.value()).thenReturn(new SoftLock<String>(null, "two", null));
    when(underlyingStore.get(eq(2L))).thenReturn(mockValueHolder);

    try {
      xaTransactionContext.commit(false);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ise) {
      // expected
    }
  }

  @Test
  public void testCommit() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));
    xaTransactionContext.addCommand(3L, new StoreEvictCommand<String>("three"));

    Store.ValueHolder<SoftLock<String>> mockValueHolder1 = mock(Store.ValueHolder.class);
    when(mockValueHolder1.value()).thenReturn(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    when(underlyingStore.get(eq(1L))).thenReturn(mockValueHolder1);
    Store.ValueHolder<SoftLock<String>> mockValueHolder2 = mock(Store.ValueHolder.class);
    when(mockValueHolder2.value()).thenReturn(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null));
    when(underlyingStore.get(eq(2L))).thenReturn(mockValueHolder2);
    Store.ValueHolder<SoftLock<String>> mockValueHolder3 = mock(Store.ValueHolder.class);
    when(mockValueHolder3.value()).thenReturn(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "three", null));
    when(underlyingStore.get(eq(3L))).thenReturn(mockValueHolder3);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L, 3L));

    when(underlyingStore.replace(any(Long.class), any(SoftLock.class), any(SoftLock.class))).thenReturn(ReplaceStatus.MISS_NOT_PRESENT);
    when(underlyingStore.remove(any(Long.class), any(SoftLock.class))).thenReturn(RemoveStatus.KEY_MISSING);

    xaTransactionContext.commit(false);
    verify(journal, times(1)).saveCommitted(eq(new TransactionId(new TestXid(0, 0))), eq(false));
    verify(journal, times(0)).saveRolledBack(eq(new TransactionId(new TestXid(0, 0))), anyBoolean());
    verify(journal, times(0)).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), anyCollection());

    verify(underlyingStore, times(1)).get(1L);
    verify(underlyingStore, times(1)).replace(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "one", new XAValueHolder<String>("un", timeSource.getTimeMillis()))), eq(new SoftLock<String>(null, "un", null)));
    verify(underlyingStore, times(1)).get(2L);
    verify(underlyingStore, times(1)).remove(eq(2L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)));
    verify(underlyingStore, times(1)).get(3L);
    verify(underlyingStore, times(1)).remove(eq(3L));
  }

  @Test
  public void testCommitInOnePhasePreparedThrows() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);

    try {
      xaTransactionContext.commitInOnePhase();
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCommitInOnePhase() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>(null, new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));
    xaTransactionContext.addCommand(3L, new StoreEvictCommand<String>("three"));

    Store.ValueHolder<SoftLock<String>> mockValueHolder = mock(Store.ValueHolder.class);
    when(mockValueHolder.value()).thenReturn(new SoftLock<String>(null, "two", null));
    when(underlyingStore.get(eq(2L))).thenReturn(mockValueHolder);

    final AtomicReference<Collection<Long>> savedInDoubtCollectionRef = new AtomicReference<Collection<Long>>();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        savedInDoubtCollectionRef.set(new HashSet<Long>((Collection<Long>) invocation.getArguments()[1]));
        return null;
      }
    }).when(journal).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), anyCollection());
    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return savedInDoubtCollectionRef.get() != null;
      }
    });
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return savedInDoubtCollectionRef.get();
      }
    });
    final AtomicReference<SoftLock> softLock1Ref = new AtomicReference<SoftLock>();
    when(underlyingStore.get(eq(1L))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return softLock1Ref.get() == null ? null : new AbstractValueHolder(-1, -1) {
          @Override
          public Object value() {
            return softLock1Ref.get();
          }
          @Override
          protected TimeUnit nativeTimeUnit() {
            return TimeUnit.MILLISECONDS;
          }
        };
      }
    });
    when(underlyingStore.putIfAbsent(eq(1L), isA(SoftLock.class))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        softLock1Ref.set((SoftLock) invocation.getArguments()[1]);
        return null;
      }
    });
    when(underlyingStore.replace(eq(1L), isA(SoftLock.class), isA(SoftLock.class))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (softLock1Ref.get() != null) {
          return ReplaceStatus.HIT;
        }
        return ReplaceStatus.MISS_PRESENT;
      }
    });
    final AtomicReference<SoftLock> softLock2Ref = new AtomicReference<SoftLock>(new SoftLock(null, "two", null));
    when(underlyingStore.get(eq(2L))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return softLock2Ref.get() == null ? null : new AbstractValueHolder(-1, -1) {
          @Override
          public Object value() {
            return softLock2Ref.get();
          }
          @Override
          protected TimeUnit nativeTimeUnit() {
            return TimeUnit.MILLISECONDS;
          }
        };
      }
    });
    when(underlyingStore.replace(eq(2L), isA(SoftLock.class), isA(SoftLock.class))).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        softLock2Ref.set((SoftLock) invocation.getArguments()[2]);
        return ReplaceStatus.HIT;
      }
    });

    when(underlyingStore.remove(any(Long.class), any(SoftLock.class))).thenReturn(RemoveStatus.REMOVED);

    xaTransactionContext.commitInOnePhase();

    Assert.assertThat(savedInDoubtCollectionRef.get(), containsInAnyOrder(1L, 2L, 3L));

    verify(journal, times(1)).saveCommitted(eq(new TransactionId(new TestXid(0, 0))), eq(false));
    verify(journal, times(0)).saveRolledBack(eq(new TransactionId(new TestXid(0, 0))), anyBoolean());
    verify(journal, times(1)).saveInDoubt(eq(new TransactionId(new TestXid(0, 0))), anyCollection());

    verify(underlyingStore, times(1)).putIfAbsent(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), null, new XAValueHolder<String>("un", timeSource.getTimeMillis()))));
    verify(underlyingStore, times(1)).replace(eq(2L), eq(new SoftLock<String>(null, "two", null)), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)));
    verify(underlyingStore, times(1)).remove(eq(3L));

    verify(underlyingStore, times(1)).get(1L);
    verify(underlyingStore, times(1)).replace(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), null, new XAValueHolder<String>("un", timeSource.getTimeMillis()))), eq(new SoftLock<String>(null, "un", null)));
    verify(underlyingStore, times(1)).get(2L);
    verify(underlyingStore, times(1)).remove(eq(2L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)));
    verify(underlyingStore, times(1)).get(3L);
    verify(underlyingStore, times(1)).remove(eq(3L));
  }

  @Test
  public void testRollbackPhase1() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));

    xaTransactionContext.rollback(false);

    verifyNoMoreInteractions(underlyingStore);
  }

  @Test
  public void testRollbackPhase2() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L));

    when(underlyingStore.get(1L)).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "one", new XAValueHolder<String>("un", timeSource.getTimeMillis()));
      }
    });
    when(underlyingStore.get(2L)).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null);
      }
    });

    when(underlyingStore.replace(any(Long.class), any(SoftLock.class), any(SoftLock.class))).thenReturn(ReplaceStatus.HIT);
    xaTransactionContext.rollback(false);

    verify(underlyingStore, times(1)).get(1L);
    verify(underlyingStore, times(1)).replace(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "one", new XAValueHolder<String>("un", timeSource.getTimeMillis()))), eq(new SoftLock<String>(null, "one", null)));
    verify(underlyingStore, times(1)).get(2L);
    verify(underlyingStore, times(1)).replace(eq(2L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)), eq(new SoftLock<String>(null, "two", null)));
  }

  @Test
  public void testCommitInOnePhaseTimeout() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));

    timeSource.advanceTime(30000);

    try {
      xaTransactionContext.commitInOnePhase();
      fail("expected TransactionTimeoutException");
    } catch (XATransactionContext.TransactionTimeoutException tte) {
      // expected
    }
  }

  @Test
  public void testPrepareTimeout() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));

    timeSource.advanceTime(30000);

    try {
      xaTransactionContext.prepare();
      fail("expected TransactionTimeoutException");
    } catch (XATransactionContext.TransactionTimeoutException tte) {
      // expected
    }
  }

  @Test
  public void testCommitConflictsEvicts() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L));
    when(underlyingStore.get(eq(1L))).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old1", new XAValueHolder<String>("new1", timeSource.getTimeMillis()));
      }
    });
    when(underlyingStore.get(eq(2L))).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old2", null);
      }
    });

    when(underlyingStore.replace(any(Long.class), any(SoftLock.class), any(SoftLock.class))).thenReturn(ReplaceStatus.MISS_NOT_PRESENT);
    when(underlyingStore.remove(any(Long.class), any(SoftLock.class))).thenReturn(RemoveStatus.KEY_MISSING);

    xaTransactionContext.commit(false);

    verify(underlyingStore, times(1)).replace(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old1", new XAValueHolder<String>("new1", timeSource.getTimeMillis()))), eq(new SoftLock<String>(null, "new1", null)));
    verify(underlyingStore, times(1)).remove(eq(1L));
    verify(underlyingStore, times(1)).remove(eq(2L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old2", null)));
    verify(underlyingStore, times(1)).remove(eq(2L));
  }

  @Test
  public void testPrepareConflictsEvicts() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    xaTransactionContext.addCommand(1L, new StorePutCommand<String>("one", new XAValueHolder<String>("un", timeSource.getTimeMillis())));
    xaTransactionContext.addCommand(2L, new StoreRemoveCommand<String>("two"));

    when(underlyingStore.replace(any(Long.class), any(SoftLock.class), any(SoftLock.class))).thenReturn(ReplaceStatus.MISS_NOT_PRESENT);

    xaTransactionContext.prepare();

    verify(underlyingStore).replace(eq(1L), eq(new SoftLock<String>(null, "one", null)), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "one", new XAValueHolder<String>("un", timeSource.getTimeMillis()))));
    verify(underlyingStore).remove(1L);
    verify(underlyingStore).replace(eq(2L), eq(new SoftLock<String>(null, "two", null)), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "two", null)));
    verify(underlyingStore).remove(2L);
  }

  @Test
  public void testRollbackConflictsEvicts() throws Exception {
    Store<Long, SoftLock<String>> underlyingStore = mock(Store.class);
    Journal<Long> journal = mock(Journal.class);
    final TestTimeSource timeSource = new TestTimeSource();

    XATransactionContext<Long, String> xaTransactionContext = new XATransactionContext<Long, String>(new TransactionId(new TestXid(0, 0)), underlyingStore, journal, timeSource, timeSource.getTimeMillis() + 30000);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L));
    when(underlyingStore.get(eq(1L))).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old1", new XAValueHolder<String>("new1", timeSource.getTimeMillis()));
      }
    });
    when(underlyingStore.get(eq(2L))).thenReturn(new AbstractValueHolder<SoftLock<String>>(-1, -1) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }
      @Override
      public SoftLock<String> value() {
        return new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old2", null);
      }
    });

    when(underlyingStore.replace(any(Long.class), any(SoftLock.class), any(SoftLock.class))).thenReturn(ReplaceStatus.MISS_NOT_PRESENT);
    when(underlyingStore.remove(any(Long.class), any(SoftLock.class))).thenReturn(RemoveStatus.KEY_MISSING);

    xaTransactionContext.rollback(false);

    verify(underlyingStore, times(1)).replace(eq(1L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old1", new XAValueHolder<String>("new1", timeSource.getTimeMillis()))), eq(new SoftLock<String>(null, "old1", null)));
    verify(underlyingStore, times(1)).remove(eq(1L));
    verify(underlyingStore, times(1)).replace(eq(2L), eq(new SoftLock<String>(new TransactionId(new TestXid(0, 0)), "old2", null)), eq(new SoftLock<String>(null, "old2", null)));
    verify(underlyingStore, times(1)).remove(eq(2L));
  }


  private static <T> Matcher<Collection<T>> isACollectionThat(
      final Matcher<Iterable<? extends T>> matcher) {
    return new BaseMatcher<Collection<T>>() {
      @Override public boolean matches(Object item) {
        return matcher.matches(item);
      }

      @Override public void describeTo(Description description) {
        matcher.describeTo(description);
      }
    };
  }

}
