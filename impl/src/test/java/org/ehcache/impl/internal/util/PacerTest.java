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
package org.ehcache.impl.internal.util;

import org.ehcache.core.spi.time.TimeSource;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class PacerTest {

  private static final int CALL_INDEX = 0;
  private static final int OR_ELSE_INDEX = 1;

  private final long now = System.currentTimeMillis();

  private int[] counters = { 0, 0};

  private Pacer pacer;

  private AtomicLong time = new AtomicLong(now);

  @Before
  public void before() {
    TimeSource source = () -> time.get();
    pacer = new Pacer(source, 10);
  }

  @Test
  public void pacedCall() {
    callAndAssert(1, 0);
    callAndAssert(1, 1);

    time.set(now + 9);

    callAndAssert(1, 2);

    time.set(now + 10);

    callAndAssert(2, 2);

    time.set(now + 21);

    callAndAssert(3, 2);
  }

  private void callAndAssert(int callCalls, int orElseCalls) {
    Runnable call = () -> counters[CALL_INDEX]++;
    Runnable orElse = () -> counters[OR_ELSE_INDEX]++;

    pacer.pacedCall(call, orElse);

    assertThat(counters[CALL_INDEX]).isEqualTo(callCalls);
    assertThat(counters[OR_ELSE_INDEX]).isEqualTo(orElseCalls);
  }
}
