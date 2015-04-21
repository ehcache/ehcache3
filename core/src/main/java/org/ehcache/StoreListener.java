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

package org.ehcache;

import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEvents;
import org.ehcache.events.StoreEventListener;

/**
 * @author rism
 */
public final class StoreListener<K, V> implements StoreEventListener<K, V> {

  private CacheEventNotificationService<K, V> eventNotificationService;
  private Cache<K, V> source;

  @Override
  public void onEviction(Cache.Entry<K, V> entry) {
    eventNotificationService.onEvent(CacheEvents.eviction(entry, this.source));
  }

  @Override
  public void onExpiration(Cache.Entry<K, V> entry) {
    eventNotificationService.onEvent(CacheEvents.expiry(entry, this.source));
  }

  public void setEventNotificationService(CacheEventNotificationService<K, V> eventNotificationService) {
    this.eventNotificationService = eventNotificationService;
  }

  public void setSource(Cache<K, V> source) {
    this.source = source;
  }
}