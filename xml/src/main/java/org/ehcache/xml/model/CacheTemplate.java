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

package org.ehcache.xml.model;

import org.ehcache.config.ResourcePool;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.CacheServiceConfigurationParser;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import javax.xml.bind.Unmarshaller;

public interface CacheTemplate {

  String keyType();

  String keySerializer();

  String keyCopier();

  String valueType();

  String valueSerializer();

  String valueCopier();

  String evictionAdvisor();

  Expiry expiry();

  String loaderWriter();

  String resilienceStrategy();

  ListenersConfig listenersConfig();

  Iterable<? extends ServiceConfiguration<?>> serviceConfigs();

  Collection<ResourcePool> resourcePools();

  CacheLoaderWriterType.WriteBehind writeBehind();

  DiskStoreSettingsType diskStoreSettings();

  SizeOfEngineLimits heapStoreSettings();

  class Impl extends CacheSpec {
    public Impl(Map<URI, CacheServiceConfigurationParser<?>> serviceConfigParsers,
                Map<URI, CacheResourceConfigurationParser> resourceConfigParsers, Unmarshaller unmarshaller, CacheTemplateType cacheTemplateType) {
      super(serviceConfigParsers, resourceConfigParsers, unmarshaller, cacheTemplateType);
    }
  }

}
