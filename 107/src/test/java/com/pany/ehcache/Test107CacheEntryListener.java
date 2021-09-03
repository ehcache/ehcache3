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

package com.pany.ehcache;

import java.util.ArrayList;
import java.util.List;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;

/**
 * TestCacheEntryListener
 */
public class Test107CacheEntryListener implements CacheEntryCreatedListener<String, String> {
  public static List<CacheEntryEvent<? extends String, ? extends String>> seen = new ArrayList<>();

  @Override
  public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents) throws CacheEntryListenerException {
    for (CacheEntryEvent<? extends String, ? extends String> cacheEntryEvent : cacheEntryEvents) {
      seen.add(cacheEntryEvent);
    }
  }
}
