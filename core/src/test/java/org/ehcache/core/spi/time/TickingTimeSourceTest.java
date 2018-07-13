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

package org.ehcache.core.spi.time;

import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TickingTimeSourceTest {

  private final TickingTimeSource tickingTimeSource = new TickingTimeSource(1L, 1000L);

  @After
  public void after() {
    tickingTimeSource.stop();
  }

  @Test
  public void getTimeMillis() {

    // Hard to test but let's just say...
    long currentTime = System.currentTimeMillis();
    tickingTimeSource.start(null);

    long actualTime = tickingTimeSource.getTimeMillis();

    // ... that time should start now...
    assertThat(actualTime).isGreaterThanOrEqualTo(currentTime);

    // ... and increase
    long end = System.currentTimeMillis() + 30_000; // 30 seconds should be way way enough to at least tick of one millisecond
    while(System.currentTimeMillis() < end) {
      if(tickingTimeSource.getTimeMillis() > actualTime) {
        break;
      }
    }

    assertThat(tickingTimeSource.getTimeMillis()).isGreaterThan(actualTime);
  }
}
