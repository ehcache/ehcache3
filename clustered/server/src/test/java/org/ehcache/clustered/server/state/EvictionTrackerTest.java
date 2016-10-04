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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;

public class EvictionTrackerTest {

  @Test
  public void testEvictionTracker() {
    EvictionTracker evictionTracker = new EvictionTracker();

    Random random = new Random();

    LongStream longStream = random.longs(1, 100);

    Set<Long> trackedOnes = new HashSet<>();

    longStream.distinct().limit(10).forEach(x -> {
      evictionTracker.add(x);
      trackedOnes.add(x);
    });

    assertThat(evictionTracker.getTrackedEvictedKeys(), containsInAnyOrder(trackedOnes.toArray()));
    assertThat(evictionTracker.getTrackedEvictedKeys(), iterableWithSize(0));


  }

}
