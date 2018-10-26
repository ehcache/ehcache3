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

package org.ehcache.xml.provider;

import org.ehcache.config.Configuration;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.ThreadPoolsType;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;

public class PooledExecutionServiceConfigurationParserTest {

  @Test
  public void parseServiceCreationConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    Configuration xmlConfig = new XmlConfiguration(getClass().getResource("/configs/thread-pools.xml"));

    assertThat(xmlConfig.getServiceCreationConfigurations()).hasSize(1);

    ServiceCreationConfiguration<?> configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();
    assertThat(configuration).isExactlyInstanceOf(PooledExecutionServiceConfiguration.class);

    PooledExecutionServiceConfiguration providerConfiguration = (PooledExecutionServiceConfiguration) configuration;
    assertThat(providerConfiguration.getPoolConfigurations()).containsKeys("big", "small");

    PooledExecutionServiceConfiguration.PoolConfiguration small = providerConfiguration.getPoolConfigurations().get("small");
    assertThat(small.minSize()).isEqualTo(1);
    assertThat(small.maxSize()).isEqualTo(1);

    PooledExecutionServiceConfiguration.PoolConfiguration big = providerConfiguration.getPoolConfigurations().get("big");
    assertThat(big.minSize()).isEqualTo(4);
    assertThat(big.maxSize()).isEqualTo(32);

    assertThat(providerConfiguration.getDefaultPoolAlias()).isEqualTo("big");
  }

  @Test
  public void unparseServiceCreationConfiguration() {
    PooledExecutionServiceConfiguration providerConfig = new PooledExecutionServiceConfiguration();
    providerConfig.addDefaultPool("foo", 5, 9);
    providerConfig.addPool("bar", 2, 6);

    Configuration config = ConfigurationBuilder.newConfigurationBuilder().addService(providerConfig).build();
    ConfigType configType = new ConfigType();
    configType = new PooledExecutionServiceConfigurationParser().unparseServiceCreationConfiguration(config, configType);

    List<ThreadPoolsType.ThreadPool> threadPools = configType.getThreadPools().getThreadPool();
    assertThat(threadPools).hasSize(2);
    threadPools.forEach(pool -> {
      if (pool.getAlias().equals("foo")) {
        assertThat(pool.getMinSize()).isEqualTo(5);
        assertThat(pool.getMaxSize()).isEqualTo(9);
        assertThat(pool.isDefault()).isEqualTo(true);
      } else if (pool.getAlias().equals("bar")) {
        assertThat(pool.getMinSize()).isEqualTo(2);
        assertThat(pool.getMaxSize()).isEqualTo(6);
        assertThat(pool.isDefault()).isEqualTo(false);
      } else {
        throw new AssertionError("Not expected");
      }
    });
  }
}
