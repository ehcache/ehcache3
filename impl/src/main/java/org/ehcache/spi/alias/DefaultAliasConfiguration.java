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

package org.ehcache.spi.alias;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Mathieu Carbou
 */
public class DefaultAliasConfiguration implements AliasConfiguration {

  private static final AtomicLong COUNTER = new AtomicLong();
  private final String cacheManagerAlias;

  public DefaultAliasConfiguration() {
    this(null);
  }

  public DefaultAliasConfiguration(String name) {
    this.cacheManagerAlias = name != null && name.trim().length() > 0 ? name : "cache-manager-" + COUNTER.incrementAndGet();
  }

  @Override
  public Class<AliasService> getServiceType() {
    return AliasService.class;
  }

  @Override
  public String getCacheManagerAlias() {
    return cacheManagerAlias;
  }

  public static DefaultAliasConfiguration cacheManagerAlias(String alias) {
    return new DefaultAliasConfiguration(alias);
  }

}
