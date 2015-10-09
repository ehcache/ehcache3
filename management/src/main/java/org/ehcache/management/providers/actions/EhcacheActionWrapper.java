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
package org.ehcache.management.providers.actions;

import org.ehcache.Cache;
import org.ehcache.Ehcache;
import org.ehcache.management.annotations.Exposed;
import org.ehcache.management.annotations.Named;

import static org.ehcache.management.utils.ConversionHelper.convert;

/**
 * @author Ludovic Orban
 */
public class EhcacheActionWrapper {

  private final Cache cache;

  public EhcacheActionWrapper(Cache cache) {
    this.cache = cache;
  }

  @Exposed
  public void clear() {
    cache.clear();
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public Object get(@Named("key") Object key) {
    Object convertedKey = convert(key, cache.getRuntimeConfiguration().getKeyType());
    return cache.get(convertedKey);
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public void remove(@Named("key") Object key) {
    Object convertedKey = convert(key, cache.getRuntimeConfiguration().getKeyType());
    cache.remove(convertedKey);
  }

  @SuppressWarnings("unchecked")
  @Exposed
  public void put(@Named("key") Object key, @Named("value") Object value) {
    Object convertedKey = convert(key, cache.getRuntimeConfiguration().getKeyType());
    Object convertedValue = convert(value, cache.getRuntimeConfiguration().getValueType());
    cache.put(convertedKey, convertedValue);
  }

}
