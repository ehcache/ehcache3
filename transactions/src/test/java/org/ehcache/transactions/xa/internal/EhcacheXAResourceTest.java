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

import org.ehcache.core.spi.store.Store;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.ehcache.transactions.xa.utils.TestXid;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class EhcacheXAResourceTest {

  @Mock
  private Store<Long, SoftLock<String>> underlyingStore;
  @Mock
  private Journal<Long> journal;
  @Mock
  private XATransactionContextFactory<Long, String> xaTransactionContextFactory;
  @Mock
  private XATransactionContext<Long, String> xaTransactionContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testStartEndWorks() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    xaResource.end(new TestXid(0, 0), XAResource.TMSUCCESS);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 1))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 1), XAResource.TMNOFLAGS);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 1))))).thenReturn(xaTransactionContext);
    xaResource.end(new TestXid(0, 1), XAResource.TMSUCCESS);
  }

  @Test
  public void testTwoNonEndedStartsFails() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(1, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    try {
      xaResource.start(new TestXid(1, 0), XAResource.TMNOFLAGS);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testEndWithoutStartFails() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    try {
      xaResource.end(new TestXid(0, 0), XAResource.TMSUCCESS);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testJoinWorks() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    xaResource.end(new TestXid(0, 0), XAResource.TMSUCCESS);

    xaResource.start(new TestXid(0, 0), XAResource.TMJOIN);
    xaResource.end(new TestXid(0, 0), XAResource.TMSUCCESS);
  }

  @Test
  public void testRecoverReportsAbortedTx() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.recover()).thenReturn(Collections.singletonMap(new TransactionId(new TestXid(0, 0)), (Collection<Long>) Arrays.asList(1L, 2L, 3L)));

    Xid[] recovered = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    assertThat(recovered.length, is(1));
    assertThat(new SerializableXid(recovered[0]), Matchers.<Xid>equalTo(new SerializableXid(new TestXid(0, 0))));
  }

  @Test
  public void testRecoverIgnoresInFlightTx() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.recover()).thenReturn(Collections.singletonMap(new TransactionId(new TestXid(0, 0)), (Collection<Long>) Arrays.asList(1L, 2L, 3L)));
    when(xaTransactionContextFactory.contains(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);

    Xid[] recovered = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    assertThat(recovered.length, is(0));
  }

  @Test
  public void testCannotPrepareUnknownXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    try {
      xaResource.prepare(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testCannotPrepareNonEndedXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    try {
      xaResource.prepare(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testPrepareOk() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    when(xaTransactionContext.prepare()).thenReturn(1);

    int prepareRc = xaResource.prepare(new TestXid(0, 0));
    assertThat(prepareRc, is(XAResource.XA_OK));

    verify(xaTransactionContextFactory, times(0)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testPrepareReadOnly() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    when(xaTransactionContext.prepare()).thenReturn(0);

    int prepareRc = xaResource.prepare(new TestXid(0, 0));
    assertThat(prepareRc, is(XAResource.XA_RDONLY));

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testCannotCommitUnknownXidInFlight() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(false);
    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    doThrow(IllegalArgumentException.class).when(xaTransactionContext).commit(eq(false));

    try {
      xaResource.commit(new TestXid(0, 0), false);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testCannotCommitUnknownXidRecovered() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(false);

    try {
      xaResource.commit(new TestXid(0, 0), false);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testCannotCommit1PcUnknownXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    try {
      xaResource.commit(new TestXid(0, 0), true);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testCannotCommit1PcNonEndedXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    try {
      xaResource.commit(new TestXid(0, 0), true);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testCannotCommitNonPreparedXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    doThrow(IllegalStateException.class).when(xaTransactionContext).commit(anyBoolean());
    try {
      xaResource.commit(new TestXid(0, 0), false);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testCannotCommit1PcPreparedXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    doThrow(IllegalStateException.class).when(xaTransactionContext).commitInOnePhase();
    try {
      xaResource.commit(new TestXid(0, 0), true);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testCommit() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    xaResource.commit(new TestXid(0, 0), false);

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testCommit1Pc() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    xaResource.commit(new TestXid(0, 0), true);

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testCannotRollbackUnknownXidInFlight() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    doThrow(IllegalStateException.class).when(xaTransactionContext).rollback(eq(false));

    try {
      xaResource.rollback(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testCannotRollbackUnknownXidRecovered() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(false);

    try {
      xaResource.rollback(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testCannotRollbackNonEndedXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);
    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    try {
      xaResource.rollback(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testRollback() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    xaResource.rollback(new TestXid(0, 0));

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testForgetUnknownXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(false);

    try {
      xaResource.forget(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }
  }

  @Test
  public void testForgetInDoubtXid() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);

    try {
      xaResource.forget(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_PROTO));
    }
  }

  @Test
  public void testForget() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isHeuristicallyTerminated(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);

    xaResource.forget(new TestXid(0, 0));

    verify(journal, times(1)).forget(new TransactionId(new TestXid(0, 0)));
  }

  @Test
  public void testTimeoutStart() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);

    when(xaTransactionContext.hasTimedOut()).thenReturn(true);

    try {
      xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XA_RBTIMEOUT));
    }

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testTimeoutEndSuccess() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);

    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    when(xaTransactionContext.hasTimedOut()).thenReturn(true);
    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);

    try {
      xaResource.end(new TestXid(0, 0), XAResource.TMSUCCESS);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XA_RBTIMEOUT));
    }

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testTimeoutEndFail() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.createTransactionContext(eq(new TransactionId(new TestXid(0, 0))), refEq(underlyingStore), refEq(journal), anyInt())).thenReturn(xaTransactionContext);

    xaResource.start(new TestXid(0, 0), XAResource.TMNOFLAGS);

    when(xaTransactionContext.hasTimedOut()).thenReturn(true);
    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);

    try {
      xaResource.end(new TestXid(0, 0), XAResource.TMFAIL);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XA_RBTIMEOUT));
    }

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrepareTimeout() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    when(xaTransactionContext.prepare()).thenThrow(XATransactionContext.TransactionTimeoutException.class);

    try {
      xaResource.prepare(new TestXid(0, 0));
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XA_RBTIMEOUT));
    }

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testCommit1PcTimeout() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(xaTransactionContextFactory.get(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(xaTransactionContext);
    doThrow(XATransactionContext.TransactionTimeoutException.class).when(xaTransactionContext).commitInOnePhase();

    try {
      xaResource.commit(new TestXid(0, 0), true);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XA_RBTIMEOUT));
    }

    verify(xaTransactionContextFactory, times(1)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testRecoveryCommitOnePhaseFails() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.recover()).thenReturn(Collections.singletonMap(new TransactionId(new TestXid(0, 0)), (Collection<Long>) Arrays.asList(1L, 2L, 3L)));
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L, 3L));

    Xid[] recoveredXids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    assertThat(recoveredXids.length, is(1));

    try {
      xaResource.commit(recoveredXids[0], true);
      fail("expected XAException");
    } catch (XAException xae) {
      assertThat(xae.errorCode, is(XAException.XAER_NOTA));
    }

    verify(xaTransactionContextFactory, times(0)).destroy(eq(new TransactionId(new TestXid(0, 0))));
  }

  @Test
  public void testRecoveryCommit() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.recover()).thenReturn(Collections.singletonMap(new TransactionId(new TestXid(0, 0)), (Collection<Long>) Arrays.asList(1L, 2L, 3L)));
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L, 3L));
    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);

    Xid[] recoveredXids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    assertThat(recoveredXids.length, is(1));

    xaResource.commit(recoveredXids[0], false);

    verify(xaTransactionContextFactory, times(0)).destroy(eq(new TransactionId(new TestXid(0, 0))));
    verify(underlyingStore, times(1)).get(eq(1L));
    verify(underlyingStore, times(1)).get(eq(2L));
    verify(underlyingStore, times(1)).get(eq(3L));
  }

  @Test
  public void testRecoveryRollback() throws Exception {
    EhcacheXAResource<Long, String> xaResource = new EhcacheXAResource<Long, String>(underlyingStore, journal, xaTransactionContextFactory);

    when(journal.isInDoubt(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(true);
    when(journal.recover()).thenReturn(Collections.singletonMap(new TransactionId(new TestXid(0, 0)), (Collection<Long>) Arrays.asList(1L, 2L, 3L)));
    when(journal.getInDoubtKeys(eq(new TransactionId(new TestXid(0, 0))))).thenReturn(Arrays.asList(1L, 2L, 3L));

    Xid[] recoveredXids = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
    assertThat(recoveredXids.length, is(1));

    xaResource.rollback(recoveredXids[0]);

    verify(xaTransactionContextFactory, times(0)).destroy(eq(new TransactionId(new TestXid(0, 0))));
    verify(underlyingStore, times(1)).get(eq(1L));
    verify(underlyingStore, times(1)).get(eq(2L));
    verify(underlyingStore, times(1)).get(eq(3L));
  }

}
