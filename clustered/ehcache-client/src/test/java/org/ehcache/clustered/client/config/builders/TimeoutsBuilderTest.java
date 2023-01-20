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
package org.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.Timeouts;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.*;

public class TimeoutsBuilderTest {

  @Test
  public void build_empty() {
    Timeouts t = TimeoutsBuilder.timeouts().build();
    assertThat(t.getReadOperationTimeout()).isEqualTo(Timeouts.DEFAULT_OPERATION_TIMEOUT);
    assertThat(t.getWriteOperationTimeout()).isEqualTo(Timeouts.DEFAULT_OPERATION_TIMEOUT);
    assertThat(t.getConnectionTimeout()).isEqualTo(Timeouts.DEFAULT_CONNECTION_TIMEOUT);
  }

  @Test
  public void build_filled() {
    Timeouts t = TimeoutsBuilder.timeouts()
      .read(Duration.ofDays(1))
      .write(Duration.ofDays(2))
      .connection(Duration.ofDays(3))
      .build();
    assertThat(t.getReadOperationTimeout()).isEqualTo(Duration.ofDays(1));
    assertThat(t.getWriteOperationTimeout()).isEqualTo(Duration.ofDays(2));
    assertThat(t.getConnectionTimeout()).isEqualTo(Duration.ofDays(3));
  }

  @Test
  public void neverGoAfterInfinite() {
    Duration afterInfinite = ChronoUnit.FOREVER.getDuration();
    Timeouts t = TimeoutsBuilder.timeouts()
      .read(afterInfinite)
      .write(afterInfinite)
      .connection(afterInfinite)
      .build();
    assertThat(t.getReadOperationTimeout()).isEqualTo(Timeouts.INFINITE_TIMEOUT);
    assertThat(t.getWriteOperationTimeout()).isEqualTo(Timeouts.INFINITE_TIMEOUT);
    assertThat(t.getConnectionTimeout()).isEqualTo(Timeouts.INFINITE_TIMEOUT);
  }
}
