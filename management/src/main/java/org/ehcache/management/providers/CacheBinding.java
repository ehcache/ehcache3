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
package org.ehcache.management.providers;

import org.ehcache.Cache;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;

/**
 * Class representing an association between an object and an alias, name, identifier
 */
@RequiredContext({@Named("cacheManagerName"), @Named("cacheName")})
public final class CacheBinding {

  private final String alias;
  private final Cache cache;

  public CacheBinding(String alias, Cache<?, ?> cache) {
    if (alias == null) throw new NullPointerException();
    if (cache == null) throw new NullPointerException();
    this.alias = alias;
    this.cache = cache;
  }

  public String getAlias() {
    return alias;
  }

  public Cache getCache() {
    return cache;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CacheBinding that = (CacheBinding) o;
    return alias.equals(that.alias) && cache.equals(that.cache);
  }

  @Override
  public int hashCode() {
    int result = alias.hashCode();
    result = 31 * result + cache.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return alias;
  }
}
