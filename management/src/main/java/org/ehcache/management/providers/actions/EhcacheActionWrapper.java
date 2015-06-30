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

import org.ehcache.Ehcache;
import org.ehcache.management.annotations.Exposed;
import org.ehcache.management.annotations.Named;

import static org.ehcache.management.utils.ConversionHelper.convert;

/**
 * @author Ludovic Orban
 */
public class EhcacheActionWrapper {

  private final Ehcache ehcache;

  public EhcacheActionWrapper(Ehcache ehcache) {
    this.ehcache = ehcache;
  }

  @Exposed
  public void clear() {
    ehcache.clear();
  }

  @Exposed
  public Object get(@Named("key") Object key) {
    Object convertedKey = convert(key, ehcache.getRuntimeConfiguration().getKeyType());
    return ehcache.get(convertedKey);
  }

  @Exposed
  public void remove(@Named("key") Object key) {
    Object convertedKey = convert(key, ehcache.getRuntimeConfiguration().getKeyType());
    ehcache.remove(convertedKey);
  }

  @Exposed
  public void put(@Named("key") Object key, @Named("value") Object value) {
    Object convertedKey = convert(key, ehcache.getRuntimeConfiguration().getKeyType());
    Object convertedValue = convert(value, ehcache.getRuntimeConfiguration().getValueType());
    ehcache.put(convertedKey, convertedValue);
  }

}
