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
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;

import com.pany.ehcache.integration.TestCacheLoaderWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class DefaultCacheLoaderWriterConfigurationParserTest {

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = new XmlConfiguration(getClass().getResource("/configs/writebehind-cache.xml")).getCacheConfigurations().get("bar");
    DefaultCacheLoaderWriterConfiguration loaderWriterConfig =
      findSingletonAmongst(DefaultCacheLoaderWriterConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(loaderWriterConfig).isNotNull();
    assertThat(loaderWriterConfig.getClazz()).isEqualTo(TestCacheLoaderWriter.class);
  }

  @Test
  public void unparseServiceConfiguration() {
    CacheConfiguration<?, ?> cacheConfig =
      newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).add(new DefaultCacheLoaderWriterConfiguration(TestCacheLoaderWriter.class)).build();
    CacheType cacheType = new DefaultCacheLoaderWriterConfigurationParser().unparseServiceConfiguration(cacheConfig, new CacheType());


    assertThat(cacheType.getLoaderWriter().getClazz()).isEqualTo(TestCacheLoaderWriter.class.getName());
  }

  @Test
  public void unparseServiceConfigurationWithInstance() {
    TestCacheLoaderWriter testCacheLoaderWriter = new TestCacheLoaderWriter();
    CacheConfiguration<?, ?> cacheConfig =
      newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).add(new DefaultCacheLoaderWriterConfiguration(testCacheLoaderWriter)).build();
    assertThatExceptionOfType(XmlConfigurationException.class).isThrownBy(() ->
      new DefaultCacheLoaderWriterConfigurationParser().unparseServiceConfiguration(cacheConfig, new CacheType()))
      .withMessage("%s", "XML translation for instance based initialization for " +
                         "DefaultCacheLoaderWriterConfiguration is not supported");
  }
}
