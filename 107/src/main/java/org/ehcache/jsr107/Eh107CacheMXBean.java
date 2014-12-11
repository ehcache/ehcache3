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
package org.ehcache.jsr107;

import javax.cache.management.CacheMXBean;

/**
 * @author teck
 */
class Eh107CacheMXBean extends Eh107MXBean implements CacheMXBean {

  private final Eh107Configuration<?, ?> config;

  Eh107CacheMXBean(String cacheName, Eh107CacheManager cacheManager, Eh107Configuration<?, ?> config) {
    super(cacheName, cacheManager, "CacheConfiguration");
    this.config = config;
  }

  @Override
  public String getKeyType() {
    return config.getKeyType().getName();
  }

  @Override
  public String getValueType() {
    return config.getValueType().getName();
  }

  @Override
  public boolean isReadThrough() {
    return config.isReadThrough();
  }

  @Override
  public boolean isWriteThrough() {
    return config.isWriteThrough();
  }

  @Override
  public boolean isStoreByValue() {
    return config.isStoreByValue();
  }

  @Override
  public boolean isStatisticsEnabled() {
    return config.isStatisticsEnabled();
  }

  @Override
  public boolean isManagementEnabled() {
    return config.isManagementEnabled();
  }
}
