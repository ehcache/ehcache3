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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * DefaultWriteBehindConfigurationTest
 */
public class DefaultWriteBehindConfigurationTest {

  @Test
  public void testMinWriteDelayDefaults() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    assertThat(configuration.getMinWriteDelay(), is(0));
  }

  @Test
  public void testMaxWriteDelayDefaults() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    assertThat(configuration.getMaxWriteDelay(), is(Integer.MAX_VALUE));
  }

  @Test
  public void testMinWriteDelaySet() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    configuration.setMinWriteDelay(10);
    assertThat(configuration.getMinWriteDelay(), is(10));
  }

  @Test
  public void testMaxWriteDelaySet() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    configuration.setMaxWriteDelay(100);
    assertThat(configuration.getMaxWriteDelay(), is(100));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinWriteDelayInvalid() {
    new DefaultWriteBehindConfiguration().setMinWriteDelay(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxWriteDelayInvalid() {
    new DefaultWriteBehindConfiguration().setMaxWriteDelay(-1);
  }

  @Test
  public void testMaxWriteDelayNotLowerThanMinWriteDelay() {
    DefaultWriteBehindConfiguration defaultWriteBehindConfiguration = new DefaultWriteBehindConfiguration();
    defaultWriteBehindConfiguration.setMinWriteDelay(10);
    try {
      defaultWriteBehindConfiguration.setMaxWriteDelay(5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Maximum write delay (5) must be larger than or equal to minimum write delay (10)"));
    }
  }

  @Test
  public void testMinWriteDelayNotLargerThanMaxWriteDelays() {
    DefaultWriteBehindConfiguration defaultWriteBehindConfiguration = new DefaultWriteBehindConfiguration();
    defaultWriteBehindConfiguration.setMaxWriteDelay(5);
    try {
      defaultWriteBehindConfiguration.setMinWriteDelay(10);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Minimum write delay (10) must be smaller than or equal to maximum write delay (5)"));
    }
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