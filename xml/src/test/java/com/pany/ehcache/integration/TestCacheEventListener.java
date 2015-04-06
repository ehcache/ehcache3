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

package com.pany.ehcache.integration;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

/**
 * @author rism
 */
public class TestCacheEventListener implements CacheEventListener<Number, String> {

  public static CacheEvent<Number, String> FIRED_EVENT;
  @Override
  public void onEvent(CacheEvent<Number, String> event) {
    FIRED_EVENT = event;
  }
}