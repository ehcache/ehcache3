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

package org.ehcache.clustered.client.config;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

/**
 * Tests the functionality of {@link TimeoutDuration}.
 */
public class TimeoutDurationTest {

  @Test
  public void testEquals() throws Exception {

    List<Pair<TimeoutDuration>> equalPairs = Arrays.asList(
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(1, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(1000, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(1000000, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1000000000, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(60000000000L, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(3600000000000L, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(86400000000000L, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(1000, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1000000, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(60000000L, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(3600000000L, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(86400000000L, MICROSECONDS)),

        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1000, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(60000L, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(3600000L, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(86400000L, MILLISECONDS)),

        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(60L, SECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(3600L, SECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(86400L, SECONDS)),

        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(60L, MINUTES)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1440L, MINUTES)),

        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(24L, HOURS)),

        Pair.of(TimeoutDuration.of(7, NANOSECONDS), TimeoutDuration.of(1 * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, MICROSECONDS), TimeoutDuration.of(1000 * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, MILLISECONDS), TimeoutDuration.of(1000000 * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, SECONDS), TimeoutDuration.of(1000000000L * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, MINUTES), TimeoutDuration.of(60000000000L * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, HOURS), TimeoutDuration.of(3600000000000L * 7, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(86400000000000L * 7, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(7, MILLISECONDS), TimeoutDuration.of(1000 * 7, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(7, SECONDS), TimeoutDuration.of(1000000 * 7, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(7, MINUTES), TimeoutDuration.of(60000000L * 7, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(7, HOURS), TimeoutDuration.of(3600000000L * 7, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(86400000000L * 7, MICROSECONDS)),

        Pair.of(TimeoutDuration.of(7, SECONDS), TimeoutDuration.of(1000 * 7, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(7, MINUTES), TimeoutDuration.of(60000L * 7, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(7, HOURS), TimeoutDuration.of(3600000L * 7, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(86400000L * 7, MILLISECONDS)),

        Pair.of(TimeoutDuration.of(7, MINUTES), TimeoutDuration.of(60L * 7, SECONDS)),
        Pair.of(TimeoutDuration.of(7, HOURS), TimeoutDuration.of(3600L * 7, SECONDS)),
        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(86400L * 7, SECONDS)),

        Pair.of(TimeoutDuration.of(7, HOURS), TimeoutDuration.of(60L * 7, MINUTES)),
        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(1440L * 7, MINUTES)),

        Pair.of(TimeoutDuration.of(7, DAYS), TimeoutDuration.of(24L * 7, HOURS)),

        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(1, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1, SECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(1, MINUTES)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, HOURS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, DAYS)),

        Pair.of(TimeoutDuration.of(0, NANOSECONDS), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, MICROSECONDS), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, MILLISECONDS), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, SECONDS), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, MINUTES), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, HOURS), TimeoutDuration.of(0, NANOSECONDS)),
        Pair.of(TimeoutDuration.of(0, DAYS), TimeoutDuration.of(0, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, DAYS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, HOURS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MINUTES), TimeoutDuration.of(Long.MAX_VALUE, MINUTES)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, SECONDS), TimeoutDuration.of(Long.MAX_VALUE, SECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS), TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS))
    );

    for (final Pair<TimeoutDuration> pair : equalPairs) {
      assertThat(pair.getFirst(), is(equalTo(pair.getSecond())));
      assertThat(pair.getSecond(), is(equalTo(pair.getFirst())));
      assertThat(pair.getFirst().hashCode(), is(equalTo(pair.getSecond().hashCode())));
    }

    List<Pair<TimeoutDuration>> unEqualPairs = Arrays.asList(
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, HOURS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, MINUTES)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, SECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, DAYS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, MINUTES)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, SECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, HOURS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MINUTES), TimeoutDuration.of(Long.MAX_VALUE, SECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MINUTES), TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MINUTES), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MINUTES), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, SECONDS), TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, SECONDS), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, SECONDS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS), TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MILLISECONDS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(Long.MAX_VALUE, MICROSECONDS), TimeoutDuration.of(Long.MAX_VALUE, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, HOURS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, MINUTES)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, SECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, DAYS), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, MINUTES)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, SECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(1, SECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(1, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(1, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(1, NANOSECONDS)),

        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, MICROSECONDS)),
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, SECONDS)),
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, MINUTES)),
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, HOURS)),
        Pair.of(TimeoutDuration.of(1, NANOSECONDS), TimeoutDuration.of(0, DAYS)),

        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(0, MILLISECONDS)),
        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(0, SECONDS)),
        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(0, MINUTES)),
        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(0, HOURS)),
        Pair.of(TimeoutDuration.of(1, MICROSECONDS), TimeoutDuration.of(0, DAYS)),

        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(0, SECONDS)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(0, MINUTES)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(0, HOURS)),
        Pair.of(TimeoutDuration.of(1, MILLISECONDS), TimeoutDuration.of(0, DAYS)),

        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(0, MINUTES)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(0, HOURS)),
        Pair.of(TimeoutDuration.of(1, SECONDS), TimeoutDuration.of(0, DAYS)),

        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(0, HOURS)),
        Pair.of(TimeoutDuration.of(1, MINUTES), TimeoutDuration.of(0, DAYS)),

        Pair.of(TimeoutDuration.of(1, HOURS), TimeoutDuration.of(0, DAYS))
    );

    for (final Pair<TimeoutDuration> pair : unEqualPairs) {
      assertThat(pair.getFirst(), is(not(equalTo(pair.getSecond()))));
      assertThat(pair.getSecond(), is(not(equalTo(pair.getFirst()))));
    }
  }

  private static final class Pair<T> {
    private final T first;
    private final T second;

    private Pair(T first, T second) {
      this.first = first;
      this.second = second;
    }

    public static <T> Pair<T> of(T first, T second) {
      return new Pair<T>(first, second);
    }

    public T getFirst() {
      return this.first;
    }

    public T getSecond() {
      return this.second;
    }
  }
}