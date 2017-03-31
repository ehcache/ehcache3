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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MessageTrackerTest {

  @Test
  public void testMessageTrackAndApplySingleThreaded() throws Exception {

    long[] input = getInputFor(0, 20);

    MessageTracker messageTracker = new MessageTracker(false);

    for (int i = 0; i < input.length; i++) {
      messageTracker.track(input[i]);
    }

    assertHighestContiguousMsgId(messageTracker, 19);

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).forEach(msg -> assertThat(messageTracker.seen(msg), is(true)));

  }

  @Test
  public void testMessageTrackAndApplyMultiThreaded() throws Exception {

    long[] input = getInputFor(0, 1000);
    final MessageTracker messageTracker = new MessageTracker(false);

    ExecutorService executorService = Executors.newWorkStealingPool();

    List<Future<?>> results = new ArrayList<>();

    for (int i = 0; i < 50 ; i++) {
      int start = 20*i;
      int end = start + 20;
      results.add(executorService.submit(() -> {
        for (int j = start; j < end; j++) {
          messageTracker.track(input[j]);
        }
        return null;
      }));
    }

    for (Future f : results) {
      f.get();
    }

    assertThat(messageTracker.seen(22), is(true));

    assertHighestContiguousMsgId(messageTracker, 999);

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).forEach(msg -> assertThat(messageTracker.seen(msg), is(true)));

  }

  @Test
  public void testDuplicateMessagesForTrackedMessages() throws Exception {

    Random random = new Random();
    long[] input = getInputFor(0, 1000);
    final MessageTracker messageTracker = new MessageTracker(false);

    Set<Long> nonTrackedMsgs = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    ExecutorService executorService = Executors.newWorkStealingPool();

    List<Future<?>> results = new ArrayList<>();

    for (int i = 0; i < 50 ; i++) {
      int start = 20*i;
      int end = start + 20;
      int randomBreakingPoint = end - 1 - random.nextInt(5);
      results.add(executorService.submit(() -> {
        for (int j = start; j < end; j++) {
          if (j < randomBreakingPoint) {
            messageTracker.track(input[j]);
          } else {
            nonTrackedMsgs.add(input[j]);
          }
        }
        return null;
      }));
    }

    for (Future f : results) {
      f.get();
    }

    long highestId = getHighestId(messageTracker);

    nonTrackedMsgs.removeIf(x -> x <= highestId);

    nonTrackedMsgs.forEach(x -> assertThat(messageTracker.seen(x), is(false)));
    LongStream.of(input).filter(x -> !nonTrackedMsgs.contains(x)).forEach(x -> assertThat(messageTracker.seen(x), is(true)));
  }

  @Test
  public void testMessageTrackWhenPassiveStartsAfterActiveMovedOn() throws Exception {

    long[] input = getInputFor(200, 500);

    MessageTracker messageTracker = new MessageTracker(true);

    for (int i = 0; i < input.length; i++) {
      messageTracker.track(input[i]);
    }

    assertHighestContiguousMsgId(messageTracker, 499);

    assertThat(messageTracker.isEmpty(), is(true));

    LongStream.of(input).forEach(msg -> assertThat(messageTracker.seen(msg), is(true)));

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

  private static void assertHighestContiguousMsgId(MessageTracker messageTracker, long highestContiguousMsgId) throws NoSuchFieldException, IllegalAccessException {
    assertThat(getHighestId(messageTracker), is(highestContiguousMsgId));
  }

  private static Long getHighestId(MessageTracker messageTracker) throws NoSuchFieldException, IllegalAccessException {
    Field entity = messageTracker.getClass().getDeclaredField("highestContiguousMsgId");
    entity.setAccessible(true);
    return (Long)entity.get(messageTracker);
  }

}
