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
package org.ehcache.transactions;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import java.net.URL;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class XmlConfigTest {

  @Test
  public void testSimpleConfig() throws Exception {
    TransactionManagerServices.getConfiguration().setJournal("null").setServerId("XmlConfigTest");
    BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();

    final URL myUrl = this.getClass().getResource("/configs/simple-xa.xml");
    Configuration xmlConfig = new XmlConfiguration(myUrl);
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(xmlConfig);
    myCacheManager.init();

    myCacheManager.close();
    transactionManager.shutdown();
  }

  @Test
  public void testTemplateConfigOverride() throws Exception {
    final URL myUrl = this.getClass().getResource("/configs/template-xa.xml");
    Configuration xmlConfig = new XmlConfiguration(myUrl);
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfig.getCacheConfigurations().get("xaCache1");
    XAStoreConfiguration xaStoreConfiguration = ServiceLocator.findSingletonAmongst(XAStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertThat(xaStoreConfiguration.getUniqueXAResourceId(), is("xaCache1"));
  }
}
