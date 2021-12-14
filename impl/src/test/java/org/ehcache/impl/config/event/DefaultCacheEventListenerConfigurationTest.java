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

package org.ehcache.impl.config.event;

import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.junit.Test;

import java.util.Set;

import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

/**
 * DefaultCacheEventListenerConfigurationTest
 */
public class DefaultCacheEventListenerConfigurationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testFailsToConstructWithEmptyEventSetAndInstance() {
    Set<EventType> fireOn = emptySet();
    new DefaultCacheEventListenerConfiguration(fireOn, mock(CacheEventListener.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsToConstructWithEmptyEventSetAndClass() {
    Set<EventType> fireOn = emptySet();
    Class<TestCacheEventListener> eventListenerClass = TestCacheEventListener.class;
    new DefaultCacheEventListenerConfiguration(fireOn, eventListenerClass);
  }

  abstract static class TestCacheEventListener implements CacheEventListener<String, String> {
  }
}
