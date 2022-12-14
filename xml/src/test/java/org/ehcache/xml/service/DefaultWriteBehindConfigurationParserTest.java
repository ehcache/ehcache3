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
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.impl.config.loaderwriter.writebehind.DefaultWriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.CacheLoaderWriterType;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class DefaultWriteBehindConfigurationParserTest {

  @Test
  public void parseServiceConfigurationNonBatching() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = new XmlConfiguration(getClass().getResource("/configs/writebehind-cache.xml")).getCacheConfigurations().get("bar");
    DefaultWriteBehindConfiguration writeBehindConfig =
      findSingletonAmongst(DefaultWriteBehindConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(writeBehindConfig).isNotNull();
    assertThat(writeBehindConfig.getConcurrency()).isEqualTo(1);
    assertThat(writeBehindConfig.getMaxQueueSize()).isEqualTo(10);
    assertThat(writeBehindConfig.getBatchingConfiguration()).isNull();
  }

  @Test
  public void parseServiceConfigurationBatching() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = new XmlConfiguration(getClass().getResource("/configs/writebehind-cache.xml")).getCacheConfigurations().get("template1");
    DefaultWriteBehindConfiguration writeBehindConfig =
      findSingletonAmongst(DefaultWriteBehindConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(writeBehindConfig).isNotNull();
    assertThat(writeBehindConfig.getConcurrency()).isEqualTo(1);
    assertThat(writeBehindConfig.getMaxQueueSize()).isEqualTo(10);
    WriteBehindConfiguration.BatchingConfiguration batchingConfiguration = writeBehindConfig.getBatchingConfiguration();
    assertThat(batchingConfiguration).isNotNull();
    assertThat(batchingConfiguration.getBatchSize()).isEqualTo(2);
    assertThat(batchingConfiguration.isCoalescing()).isEqualTo(false);
    assertThat(batchingConfiguration.getMaxDelay()).isEqualTo(10);
    assertThat(batchingConfiguration.getMaxDelayUnit()).isEqualTo(TimeUnit.SECONDS);
  }

  @Test
  public void unparseServiceConfigurationBatched() {
    WriteBehindConfiguration writeBehindConfiguration =
      WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(123, TimeUnit.SECONDS, 987)
      .enableCoalescing().concurrencyLevel(8).useThreadPool("foo").queueSize(16).build();
    CacheConfiguration<?, ?> cacheConfig = newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).add(writeBehindConfiguration).build();
    CacheType cacheType = new CacheType();
    cacheType = new DefaultWriteBehindConfigurationParser().unparseServiceConfiguration(cacheConfig, cacheType);

    CacheLoaderWriterType.WriteBehind writeBehind = cacheType.getLoaderWriter().getWriteBehind();
    assertThat(writeBehind.getThreadPool()).isEqualTo("foo");
    assertThat(writeBehind.getSize()).isEqualTo(16);
    assertThat(writeBehind.getConcurrency()).isEqualTo(8);
    CacheLoaderWriterType.WriteBehind.Batching batching = writeBehind.getBatching();
    assertThat(batching.getBatchSize()).isEqualTo(987);
    assertThat(batching.isCoalesce()).isEqualTo(true);
    assertThat(batching.getMaxWriteDelay().getValue()).isEqualTo(123);
    assertThat(batching.getMaxWriteDelay().getUnit()).isEqualTo(org.ehcache.xml.model.TimeUnit.SECONDS);
  }
}
