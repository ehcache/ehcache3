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

package org.ehcache.xml.service;

import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.DiskStoreSettingsType;

import java.math.BigInteger;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class OffHeapDiskStoreConfigurationParser
  extends SimpleCoreServiceConfigurationParser<DiskStoreSettingsType, DiskStoreSettingsType, OffHeapDiskStoreConfiguration> {

  public OffHeapDiskStoreConfigurationParser() {
    super(OffHeapDiskStoreConfiguration.class,
      CacheTemplate::diskStoreSettings,
      config -> new OffHeapDiskStoreConfiguration(config.getThreadPool(), config.getWriterConcurrency().intValue(), config.getDiskSegments().intValue()),
      CacheType::getDiskStoreSettings, CacheType::setDiskStoreSettings,
      config -> new DiskStoreSettingsType()
        .withThreadPool(config.getThreadPoolAlias())
        .withDiskSegments(BigInteger.valueOf(config.getDiskSegments()))
        .withWriterConcurrency(BigInteger.valueOf(config.getWriterConcurrency())));
  }
}
