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

package org.ehcache.events;
import org.ehcache.event.EventType;

import java.util.HashMap;
import java.util.List;

/**
 * @author rism
 */
public class EventThreadLocalImpl implements EventThreadLocal {
  private final ThreadLocal<ComplexEventThreadLocalList<CacheEventWrapper>> threadLocal
      = new ThreadLocal<ComplexEventThreadLocalList<CacheEventWrapper>>() {
    @Override
    protected ComplexEventThreadLocalList<CacheEventWrapper> initialValue() {
      return new ComplexEventThreadLocalList<CacheEventWrapper>();
    }
  };

  @Override
  public List<CacheEventWrapper> get() {
    return threadLocal.get().getEventList();
  }

  @Override
  public void addToEventList(CacheEventWrapper eventWrapper) {
    this.get().add(eventWrapper);
    markFailedCacheEvents();
  }
  
  @Override
  public void verifyOrderedDispatch(boolean ordered) {
    ComplexEventThreadLocalList<CacheEventWrapper> complexEventThreadLocalList = threadLocal.get();
    if(complexEventThreadLocalList.getEventList().isEmpty()){
      complexEventThreadLocalList.setOrdered(ordered);
      this.threadLocal.set(complexEventThreadLocalList);
    }
  }

  @Override
  public boolean isOrdered() {
    return threadLocal.get().isOrdered();
  }

  @Override
  public void cleanUp() {
    threadLocal.remove();
  }

  private void markFailedCacheEvents() {
    HashMap<Object, CacheEventWrapper> cacheEventMap = new HashMap<Object, CacheEventWrapper>();
    for (CacheEventWrapper eventWrapper : this.get()) {
      if ((eventWrapper.cacheEvent.getType() == EventType.CREATED)
          || (eventWrapper.cacheEvent.getType() == EventType.UPDATED)
          || (eventWrapper.cacheEvent.getType() == EventType.REMOVED)) {
        if (cacheEventMap.containsKey(eventWrapper.cacheEvent.getKey())) {
          cacheEventMap.remove(eventWrapper.cacheEvent.getKey()).markFailed();
        } else {
          cacheEventMap.put(eventWrapper.cacheEvent.getKey(), eventWrapper);
        }
      }
    }
  }
}
