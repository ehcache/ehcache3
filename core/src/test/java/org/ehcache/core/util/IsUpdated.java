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

package org.ehcache.core.util;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.EventType;
import org.mockito.ArgumentMatcher;

public class IsUpdated<K, V> extends ArgumentMatcher<CacheEvent<K, V>> {

  public boolean matches(Object event) {
    CacheEvent<?, ?> cacheEvent = (CacheEvent)event;
    return (cacheEvent.getType() == EventType.UPDATED);
  }
}
