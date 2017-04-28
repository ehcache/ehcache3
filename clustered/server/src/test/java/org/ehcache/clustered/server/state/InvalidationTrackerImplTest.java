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

package org.ehcache.clustered.server.state;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InvalidationTrackerImplTest {

  private final InvalidationTrackerImpl tracker = new InvalidationTrackerImpl();

  @Test
  public void clearInProgress() {
    assertThat(tracker.isClearInProgress()).isFalse();
    tracker.setClearInProgress(true);
    assertThat(tracker.isClearInProgress()).isTrue();
    tracker.setClearInProgress(false);
    assertThat(tracker.isClearInProgress()).isFalse();
  }

  @Test
  public void trackHashInvalidation_incrementOnsameValue() throws Exception {
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);

    assertInvalidationCount(5L, 3);
  }

  @Test
  public void trackHashInvalidation_differentKeysCreateDifferentCounts() throws Exception {
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(2L);
    tracker.trackHashInvalidation(2L);

    assertInvalidationCount(5L, 1);
    assertInvalidationCount(2L, 2);
  }

  @Test
  public void untrackHashInvalidation_untrackWhenNotTracked() throws Exception {
    tracker.untrackHashInvalidation(5L);
    assertInvalidationCount(5L, 0);
  }

  @Test
  public void untrackHashInvalidation_untrackDecrementCount() throws Exception {
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);

    tracker.untrackHashInvalidation(5L);
    assertInvalidationCount(5L, 2);

    tracker.untrackHashInvalidation(5L);
    assertInvalidationCount(5L, 1);

    tracker.untrackHashInvalidation(5L);
    assertInvalidationCount(5L, 0);
  }

  @Test
  public void untrackHashInvalidation_rightKeyIsDecremented() throws Exception {
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(2L);

    tracker.untrackHashInvalidation(5L);
    assertInvalidationCount(5L, 2);
    assertInvalidationCount(2L, 1);
  }

  @Test
  public void getTrackedKeys() {
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(2L);
    assertThat(tracker.getTrackedKeys()).containsOnly(5L, 2L);
  }

  @Test
  public void getTrackedKeys_notReturnedWhenUntracked() {
    tracker.trackHashInvalidation(5L);
    tracker.untrackHashInvalidation(5L);
    assertThat(tracker.getTrackedKeys()).isEmpty();
  }

  @Test
  public void clear() {
    tracker.trackHashInvalidation(5L);
    tracker.clear();
    assertInvalidationCount(5L, 0);
  }

  private void assertInvalidationCount(long chainKey, int times) {
    assertThat(tracker.getInvalidationCount(chainKey)).isEqualTo(times);
  }

}
