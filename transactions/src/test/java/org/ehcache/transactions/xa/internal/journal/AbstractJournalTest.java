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

package org.ehcache.transactions.xa.internal.journal;

import org.ehcache.transactions.xa.internal.TransactionId;
import org.ehcache.transactions.xa.utils.TestXid;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public abstract class AbstractJournalTest {

  protected Journal<Long> journal;

  @Before
  public void setUp() throws Exception {
    journal = createJournal();
    journal.open();
  }

  @After
  public void tearDown() throws Exception {
    journal.close();
  }

  @Test
  public void testGetInDoubtKeysReturnsCorrectKeysAfterSavedCollectionCleared() throws Exception {
    Collection<Long> keys = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), keys);
    keys.clear();

    Collection<Long> inDoubtKeys = journal.getInDoubtKeys(new TransactionId(new TestXid(0, 0)));
    assertThat(inDoubtKeys, containsInAnyOrder(1L, 2L, 3L));
  }

  @Test
  public void testSaveAndRecoverHappyPath() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveInDoubt(new TransactionId(new TestXid(1, 0)), Arrays.asList(4L, 5L, 6L));

    Map<TransactionId, Collection<Long>> recovered = journal.recover();
    assertThat(recovered.size(), is(2));
    assertThat(recovered.keySet(), containsInAnyOrder(new TransactionId(new TestXid(0, 0)), new TransactionId(new TestXid(1, 0))));
    assertThat(recovered.values(), containsInAnyOrder((Collection) Arrays.asList(1L, 2L, 3L), (Collection) Arrays.asList(4L, 5L, 6L)));

    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), false);

    recovered = journal.recover();
    assertThat(recovered.size(), is(1));
    assertThat(recovered.keySet(), containsInAnyOrder(new TransactionId(new TestXid(1, 0))));
    assertThat(recovered.values(), contains((Collection) Arrays.asList(4L, 5L, 6L)));

    journal.saveRolledBack(new TransactionId(new TestXid(1, 0)), false);

    recovered = journal.recover();
    assertThat(recovered.size(), is(0));
  }

  @Test
  public void testSaveInDoubtTwiceThrows() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    try {
      journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(4L, 5L, 6L));
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testSaveRolledBackTwiceWorks() throws Exception {
    journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), false);
    journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), false);
  }

  @Test
  public void testSaveCommittedTwiceWorks() throws Exception {
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), false);
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), false);
  }

  @Test
  public void testHeuristicDecisionsNotReportedByRecover() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), true);

    assertThat(journal.recover().isEmpty(), is(true));
  }

  @Test
  public void testHeuristicDecisionsReported() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveInDoubt(new TransactionId(new TestXid(1, 0)), Arrays.asList(4L, 5L, 6L));
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), true);

    assertThat(journal.heuristicDecisions().keySet(), containsInAnyOrder(new TransactionId(new TestXid(0, 0))));

    journal.saveRolledBack(new TransactionId(new TestXid(1, 0)), true);

    assertThat(journal.heuristicDecisions().keySet(), containsInAnyOrder(new TransactionId(new TestXid(0, 0)), new TransactionId(new TestXid(1, 0))));
  }

  @Test
  public void testHeuristicDecisionsForget() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveInDoubt(new TransactionId(new TestXid(1, 0)), Arrays.asList(4L, 5L, 6L));
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), true);
    journal.saveRolledBack(new TransactionId(new TestXid(1, 0)), true);

    assertThat(journal.heuristicDecisions().keySet(), containsInAnyOrder(new TransactionId(new TestXid(0, 0)), new TransactionId(new TestXid(1, 0))));

    journal.forget(new TransactionId(new TestXid(0, 0)));

    assertThat(journal.heuristicDecisions().keySet(), containsInAnyOrder(new TransactionId(new TestXid(1, 0))));

    journal.forget(new TransactionId(new TestXid(1, 0)));

    assertThat(journal.heuristicDecisions().keySet().isEmpty(), is(true));
  }

  @Test
  public void testCannotForgetUnknownTransaction() throws Exception {
    try {
      journal.forget(new TransactionId(new TestXid(0, 0)));
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCannotForgetNonHeuristicTransaction() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    try {
      journal.forget(new TransactionId(new TestXid(0, 0)));
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCannotOverwriteHeuristicCommitWithNonHeuristic() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveCommitted(new TransactionId(new TestXid(0, 0)), true);
    try {
      journal.saveCommitted(new TransactionId(new TestXid(0, 0)), false);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCannotOverwriteHeuristicRollbackWithNonHeuristic() throws Exception {
    journal.saveInDoubt(new TransactionId(new TestXid(0, 0)), Arrays.asList(1L, 2L, 3L));
    journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), true);
    try {
      journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), false);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCannotSaveCommitHeuristicWhenNoInDoubtRecordExists() throws Exception {
    try {
      journal.saveCommitted(new TransactionId(new TestXid(0, 0)), true);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testCannotSaveRollbackHeuristicWhenNoInDoubtRecordExists() throws Exception {
    try {
      journal.saveRolledBack(new TransactionId(new TestXid(0, 0)), true);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  protected abstract Journal<Long> createJournal();
}
