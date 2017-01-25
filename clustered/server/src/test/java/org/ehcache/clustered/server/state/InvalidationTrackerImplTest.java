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

import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class InvalidationTrackerImplTest {

  @Test
  public void trackHashInvalidation() throws Exception {
    InvalidationTrackerImpl tracker = new InvalidationTrackerImpl();
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(2L);
    tracker.trackHashInvalidation(4L);
    tracker.trackHashInvalidation(4L);
    ConcurrentMap<Long, Integer> invalidationMap = tracker.invalidationMap;
    assertThat(invalidationMap.get(5L), is(3));
    assertThat(invalidationMap.get(2L), is(1));
    assertThat(invalidationMap.get(4L), is(2));
  }

  @Test
  public void untrackHashInvalidation() throws Exception {
    InvalidationTrackerImpl tracker = new InvalidationTrackerImpl();
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    tracker.trackHashInvalidation(5L);
    ConcurrentMap<Long, Integer> invalidationMap = tracker.invalidationMap;
    assertThat(invalidationMap.get(5L), is(3));

    tracker.untrackHashInvalidation(5L);
    assertThat(invalidationMap.get(5L), is(2));
    tracker.untrackHashInvalidation(5L);
    assertThat(invalidationMap.get(5L), is(1));
    tracker.untrackHashInvalidation(5L);
    assertThat(invalidationMap.get(5L), nullValue());
  }

}
