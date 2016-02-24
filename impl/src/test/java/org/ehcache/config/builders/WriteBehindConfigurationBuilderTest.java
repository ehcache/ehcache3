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

package org.ehcache.config.builders;

import java.util.concurrent.TimeUnit;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

public class WriteBehindConfigurationBuilderTest {

  @Test
  public void testDefaultUnBatchedConcurrency() {
    assertThat(newUnBatchedWriteBehindConfiguration().build().getConcurrency(), is(1));
  }

  @Test
  public void testDefaultBatchedConcurrency() {
    assertThat(newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).build().getConcurrency(), is(1));
  }

  @Test
  public void testIllegalNonPositiveConcurrencyWhenUnBatched() {
    try {
      newUnBatchedWriteBehindConfiguration().concurrencyLevel(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testIllegalNonPositiveConcurrencyWhenBatched() {
    try {
      newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).concurrencyLevel(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testDefaultUnBatchedQueueSize() {
    assertThat(newUnBatchedWriteBehindConfiguration().build().getMaxQueueSize(), is(Integer.MAX_VALUE));
  }

  @Test
  public void testDefaultBatchedQueueSize() {
    assertThat(newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).build().getMaxQueueSize(), is(Integer.MAX_VALUE));
  }

  @Test
  public void testIllegalNonPositiveQueueSizeWhenUnBatched() {
    try {
      newUnBatchedWriteBehindConfiguration().queueSize(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testIllegalNonPositiveQueueSizeWhenBatched() {
    try {
      newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).queueSize(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testDefaultUnBatchedThreadPoolAlias() {
    assertThat(newUnBatchedWriteBehindConfiguration().build().getThreadPoolAlias(), nullValue());
  }

  @Test
  public void testDefaultBatchedThreadPoolAlias() {
    assertThat(newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).build().getThreadPoolAlias(), nullValue());
  }

  @Test
  public void testDefaultBatchCoalescing() {
    assertThat(newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).build().getBatchingConfiguration().isCoalescing(), is(false));
  }

  @Test
  public void testIllegalNonPositiveBatchDelay() {
    try {
      newBatchedWriteBehindConfiguration(0, TimeUnit.MINUTES, 10);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testIllegalNonPositiveBatchSize() {
    try {
      newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

}
