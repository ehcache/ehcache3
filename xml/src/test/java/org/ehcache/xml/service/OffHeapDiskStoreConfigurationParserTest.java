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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.DiskStoreSettingsType;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class OffHeapDiskStoreConfigurationParserTest extends ServiceConfigurationParserTestBase {

  public OffHeapDiskStoreConfigurationParserTest() {
    super(new OffHeapDiskStoreConfigurationParser());
  }

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = getCacheDefinitionFrom("/configs/resources-caches.xml", "tiered");
    OffHeapDiskStoreConfiguration diskConfig =
      findSingletonAmongst(OffHeapDiskStoreConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(diskConfig.getThreadPoolAlias()).isEqualTo("some-pool");
    assertThat(diskConfig.getWriterConcurrency()).isEqualTo(2);
    assertThat(diskConfig.getDiskSegments()).isEqualTo(4);
  }

  @Test
  public void unparseServiceConfiguration() {
    CacheConfiguration<?, ?> cacheConfig =
      buildCacheConfigWithServiceConfig(new OffHeapDiskStoreConfiguration("foo", 4, 8));
    CacheType cacheType = new CacheType();
    cacheType = parser.unparseServiceConfiguration(cacheConfig, cacheType);

    DiskStoreSettingsType diskStoreSettings = cacheType.getDiskStoreSettings();
    assertThat(diskStoreSettings.getThreadPool()).isEqualTo("foo");
    assertThat(diskStoreSettings.getWriterConcurrency()).isEqualTo(4);
    assertThat(diskStoreSettings.getDiskSegments()).isEqualTo(8);
  }

}
