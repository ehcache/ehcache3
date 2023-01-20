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
package org.ehcache.docs.plugs;

import org.ehcache.core.Ehcache;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class ListenerObject implements CacheEventListener<Object, Object> {
  private int evicted;
  @Override
  public void onEvent(CacheEvent<? extends Object, ? extends Object> event) {
    Logger logger = LoggerFactory.getLogger(Ehcache.class + "-" + "GettingStarted");
    logger.info(event.getType().toString());
    if(event.getType() == EventType.EVICTED){
      evicted++;
    }
  }

  public void resetEvictionCount() {
    evicted = 0;
  }

  public int evicted() {
    return evicted;
  }
}
