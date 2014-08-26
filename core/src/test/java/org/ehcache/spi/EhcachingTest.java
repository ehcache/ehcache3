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

package org.ehcache.spi;

import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Alex Snaps
 */
public class EhcachingTest implements Ehcaching {

  private final static AtomicInteger instantiationCounter = new AtomicInteger(0);
  private final static AtomicInteger creationCounter = new AtomicInteger(0);

  public EhcachingTest() {
    instantiationCounter.getAndIncrement();
  }

  @Override
  public CacheManager createCacheManager(final Configuration configuration, final ServiceProvider serviceProvider) {
    creationCounter.getAndIncrement();
    return null;
  }

  public static int getCreationCount() {
    return creationCounter.get();
  }

  public static int getInstantiationCount() {
    return instantiationCounter.get();
  }
}
