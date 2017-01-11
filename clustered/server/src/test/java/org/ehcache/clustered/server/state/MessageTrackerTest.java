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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MessageTrackerTest {

  @Test
  public void testMessageTrackAndApplySingleThreaded() throws Exception {

    long[] input = getInputFor(0, 20);

    MessageTracker messageTracker = new MessageTracker();

    for (int i = 0; i < input.length; i++) {
      messageTracker.track(input[i]);
      messageTracker.applied(input[i]);
    }

    assertLowerWaterMark(messageTracker, 19);

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).forEach(msg -> assertThat(messageTracker.shouldApply(msg), is(false)));

  }

  @Test
  public void testMessageTrackAndApplyMultiThreaded() throws Exception {

    long[] input = getInputFor(0, 1000);
    final MessageTracker messageTracker = new MessageTracker();

    ExecutorService executorService = Executors.newWorkStealingPool();

    List<Future<?>> results = new ArrayList<>();

    for (int i = 0; i < 50 ; i++) {
      int start = 20*i;
      int end = start + 20;
      results.add(executorService.submit(() -> {
        for (int j = start; j < end; j++) {
          messageTracker.track(input[j]);
          messageTracker.applied(input[j]);
        }
        return null;
      }));
    }

    for (Future f : results) {
      f.get();
    }

    assertLowerWaterMark(messageTracker, 999);

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).forEach(msg -> assertThat(messageTracker.shouldApply(msg), is(false)));

  }

  @Test
  public void testDuplicateMessagesForTrackedMessages() throws Exception {

    Random random = new Random();
    long[] input = getInputFor(0, 1000);
    final MessageTracker messageTracker = new MessageTracker();

    Set<Long> nonAppliedMsgs = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    ExecutorService executorService = Executors.newWorkStealingPool();

    List<Future<?>> results = new ArrayList<>();

    for (int i = 0; i < 50 ; i++) {
      int start = 20*i;
      int end = start + 20;
      int randomBreakingPoint = end - 1 - random.nextInt(5);
      results.add(executorService.submit(() -> {
        for (int j = start; j < end; j++) {
          messageTracker.track(input[j]);
          if (j < randomBreakingPoint) {
            messageTracker.applied(input[j]);
          } else {
            nonAppliedMsgs.add(input[j]);
          }
        }
        return null;
      }));
    }

    for (Future f : results) {
      f.get();
    }

    assertThat(messageTracker.isEmpty(), is(false));

    nonAppliedMsgs.forEach(x -> assertThat(messageTracker.shouldApply(x), is(true)));

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).filter(x -> !nonAppliedMsgs.contains(x)).forEach(x -> assertThat(messageTracker.shouldApply(x), is(false)));

  }

  /**
   *
   * @param start start of range
   * @param end exclusive
   * @return
   */
  private static long[] getInputFor(int start, int end) {
    Random random = new Random();
    return random.longs(start, end).unordered().distinct().limit(end - start).toArray();
  }

  private static void assertLowerWaterMark(MessageTracker messageTracker, long lwm) throws NoSuchFieldException, IllegalAccessException {
    Field entity = messageTracker.getClass().getDeclaredField("lowerWaterMark");
    entity.setAccessible(true);
    assertThat((Long)entity.get(messageTracker), is(lwm));
  }

}
