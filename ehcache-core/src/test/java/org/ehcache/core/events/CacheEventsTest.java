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
package org.ehcache.core.events;

import org.ehcache.Cache;
import org.ehcache.event.CacheEvent;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheEventsTest {

  @Test
  public void testToString() throws Exception {
    @SuppressWarnings("unchecked")
    Cache<String, String> cache = mock(Cache.class);
    when(cache.toString()).thenReturn("cache");
    CacheEvent<String, String> event = CacheEvents.update("key", "old", "new", cache);
    assertThat(event.toString()).isEqualTo("UPDATED on cache key,oldValue,newValue='key','old','new'");
  }

}
