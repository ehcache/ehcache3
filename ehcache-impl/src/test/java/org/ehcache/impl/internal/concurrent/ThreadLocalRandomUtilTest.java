/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2026
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

package org.ehcache.impl.internal.concurrent;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ThreadLocalRandomUtilTest {

  @Test
  public void probeIsZeroOnNewThread() throws Exception {
    assertThat(onNewThread(ThreadLocalRandomUtil::getProbe), is(0));
  }

  @Test
  public void localInitSetsNonZeroProbe() throws Exception {
    int probe = onNewThread(() -> {
      ThreadLocalRandomUtil.localInit();
      return ThreadLocalRandomUtil.getProbe();
    });

    assertThat(probe, not(0));
  }

  @Test
  public void advanceProbeUpdatesProbe() throws Exception {
    int[] probes = onNewThread(() -> {
      ThreadLocalRandomUtil.localInit();
      int initial = ThreadLocalRandomUtil.getProbe();
      int advanced = ThreadLocalRandomUtil.advanceProbe(initial);
      return new int[] { initial, advanced, ThreadLocalRandomUtil.getProbe() };
    });

    assertThat(probes[1], not(probes[0]));
    assertThat(probes[2], is(probes[1]));
  }

  private static <T> T onNewThread(Callable<T> callable) throws Exception {
    FutureTask<T> task = new FutureTask<>(callable);
    Thread thread = new Thread(task);
    thread.start();
    return task.get();
  }
}
