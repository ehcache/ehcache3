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

import org.ehcache.Cache;
import org.ehcache.config.EvictionPrioritizer;

/**
 * @author Alex Snaps
 */
public class MyEviction implements EvictionPrioritizer<Object, Object> {
  @Override
  public int compare(final Cache.Entry<Object, Object> o1, final Cache.Entry<Object, Object> o2) {
    int h1 = o1.getValue().hashCode();
    int h2 = o2.getValue().hashCode();
    return (h1 < h2) ? -1 : ((h1 == h2) ? 0 : 1);
  }
}
