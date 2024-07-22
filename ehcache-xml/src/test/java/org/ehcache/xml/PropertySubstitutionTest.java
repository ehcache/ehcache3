/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.xml;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.impl.config.loaderwriter.writebehind.DefaultWriteBehindConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Test;

import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class PropertySubstitutionTest {

  @Test
  public void testMissingProperties() {
    final URL resource = PropertySubstitutionTest.class.getResource("/configs/ehcache-system-props.xml");
    XmlConfigurationException failure = assertThrows(XmlConfigurationException.class, () -> new XmlConfiguration(resource));
    assertThat(failure.getCause().getClass().getSimpleName(), is("UnmarshalException"));
  }

  @Test
  public void testSubstitutions() {
    Map<String, String> neededProperties = new HashMap<>();
    neededProperties.put("ehcache.persistence.directory", "foobar");
    neededProperties.put("ehcache.thread-pools.min-size", "0");
    neededProperties.put("ehcache.thread-pools.max-size", "4");
    neededProperties.put("ehcache.expiry.ttl", "10");
    neededProperties.put("ehcache.expiry.tti", "20");
    neededProperties.put("ehcache.loader-writer.write-behind.size", "1000");
    neededProperties.put("ehcache.loader-writer.write-behind.concurrency", "4");
    neededProperties.put("ehcache.loader-writer.write-behind.batching.batch-size", "100");
    neededProperties.put("ehcache.loader-writer.write-behind.batching.max-write-delay", "10");
    neededProperties.put("ehcache.disk-store-settings.writer-concurrency", "8");
    neededProperties.put("ehcache.disk-store-settings.disk-segments", "16");
    neededProperties.put("ehcache.resources.heap", "1024");
    neededProperties.put("ehcache.resources.offheap", "2048");
    neededProperties.put("ehcache.resources.disk", "4096");

    System.getProperties().putAll(neededProperties);
    try {
      final URL resource = PropertySubstitutionTest.class.getResource("/configs/ehcache-system-props.xml");
      XmlConfiguration xmlConfig = new XmlConfiguration(new XmlConfiguration(resource));

      Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations = xmlConfig.getServiceCreationConfigurations();

      assertThat(findSingletonAmongst(DefaultPersistenceConfiguration.class, serviceCreationConfigurations).getRootDirectory().getAbsolutePath(), either(
        is("/dir/path/foobar/tail")).or(new CustomTypeSafeMatcher<String>("matches pattern [A-Z]:\\dir\\path\\foobar\\tail") {
        @Override
        protected boolean matchesSafely(String item) {
          return item.matches("[A-Z]:\\\\dir\\\\path\\\\foobar\\\\tail");
        }
      }));
      PooledExecutionServiceConfiguration.PoolConfiguration poolConfiguration = findSingletonAmongst(PooledExecutionServiceConfiguration.class, serviceCreationConfigurations).getPoolConfigurations().get("theone");
      assertThat(poolConfiguration.minSize(), is(0));
      assertThat(poolConfiguration.maxSize(), is(4));

      CacheConfiguration<?, ?> testCacheConfig = xmlConfig.getCacheConfigurations().get("test");
      assertThat(testCacheConfig.getExpiryPolicy().getExpiryForCreation(null, null), is(Duration.ofHours(10)));
      assertThat(testCacheConfig.getExpiryPolicy().getExpiryForAccess(null, null), is(nullValue()));
      assertThat(testCacheConfig.getExpiryPolicy().getExpiryForUpdate(null, null, null), is(Duration.ofHours(10)));

      DefaultWriteBehindConfiguration writeBehindConfiguration = findSingletonAmongst(DefaultWriteBehindConfiguration.class, testCacheConfig.getServiceConfigurations());
      assertThat(writeBehindConfiguration.getConcurrency(), is(4));
      assertThat(writeBehindConfiguration.getMaxQueueSize(), is(1000));
      assertThat(writeBehindConfiguration.getBatchingConfiguration().getBatchSize(), is(100));
      assertThat(writeBehindConfiguration.getBatchingConfiguration().getMaxDelay(), is(10L));

      OffHeapDiskStoreConfiguration diskStoreConfiguration = findSingletonAmongst(OffHeapDiskStoreConfiguration.class, testCacheConfig.getServiceConfigurations());
      assertThat(diskStoreConfiguration.getDiskSegments(), is(16));
      assertThat(diskStoreConfiguration.getWriterConcurrency(), is(8));

      assertThat(testCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), is(1024L));
      assertThat(testCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getSize(), is(2048L));
      assertThat(testCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), is(4096L));

      CacheConfiguration<?, ?> anotherTestCacheConfig = xmlConfig.getCacheConfigurations().get("another-test");
      assertThat(anotherTestCacheConfig.getExpiryPolicy().getExpiryForCreation(null, null), is(Duration.ofMillis(20)));
      assertThat(anotherTestCacheConfig.getExpiryPolicy().getExpiryForAccess(null, null), is(Duration.ofMillis(20)));
      assertThat(anotherTestCacheConfig.getExpiryPolicy().getExpiryForUpdate(null, null, null), is(Duration.ofMillis(20)));
    } finally {
      neededProperties.keySet().forEach(System::clearProperty);
    }
  }
}
