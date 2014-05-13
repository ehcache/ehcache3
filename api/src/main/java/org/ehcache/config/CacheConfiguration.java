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

package org.ehcache.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public final class CacheConfiguration<K, V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Collection<ServiceConfiguration<?>> serviceConfigurations;

  public CacheConfiguration(Class<K> keyType, Class<V> valueType, ServiceConfiguration<?>... serviceConfigurations) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return serviceConfigurations;
  }

  public Class<K> getKeyType() {
    return keyType;
  }

  public Class<V> getValueType() {
    return valueType;
  }
}
