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

package org.ehcache.config.writebehind;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * DefaultWriteBehindConfigurationTest
 */
public class DefaultWriteBehindConfigurationTest {

  @Test
  public void testMinMaxWriteDelaysDefaults() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    configuration.setWriteDelays(null, null);

    assertThat(configuration.getMinWriteDelay(), is(0));
    assertThat(configuration.getMaxWriteDelay(), is(1));
  }

  @Test
  public void testMinMaxWriteDelaysSet() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    configuration.setWriteDelays(10, 100);
    assertThat(configuration.getMinWriteDelay(), is(10));
    assertThat(configuration.getMaxWriteDelay(), is(100));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinWriteDelayInvalid() {
    new DefaultWriteBehindConfiguration().setWriteDelays(-1, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxWriteDelayInvalid() {
    new DefaultWriteBehindConfiguration().setWriteDelays(null, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxWriteDelaysNotLowerThanMinWriteDelays() {
    new DefaultWriteBehindConfiguration().setWriteDelays(10, 5);
  }

  @Test
  public void testMaxQueueSizeDefault() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    assertThat(configuration.getWriteBehindMaxQueueSize(), is(Integer.MAX_VALUE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxQueueSizeInvalid() {
    new DefaultWriteBehindConfiguration().setWriteBehindMaxQueueSize(0);
  }

}