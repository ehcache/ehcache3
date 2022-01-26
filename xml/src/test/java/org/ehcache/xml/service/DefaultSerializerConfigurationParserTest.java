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
import org.ehcache.core.util.ClassLoading;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheEntryType;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;

import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.serializer.TestSerializer3;
import com.pany.ehcache.serializer.TestSerializer4;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;

public class DefaultSerializerConfigurationParserTest {

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = new XmlConfiguration(getClass().getResource("/configs/default-serializer.xml")).getCacheConfigurations().get("foo");
    @SuppressWarnings("rawtypes")
    Collection<DefaultSerializerConfiguration> copierConfigs =
      findAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfigs).hasSize(2);

    for(DefaultSerializerConfiguration<?> copierConfig : copierConfigs) {
      if(copierConfig.getType() == DefaultSerializerConfiguration.Type.KEY) {
        assertThat(copierConfig.getClazz()).isEqualTo(TestSerializer3.class);
      } else {
        assertThat(copierConfig.getClazz()).isEqualTo(TestSerializer4.class);
      }
    }
  }

  @Test
  public void unparseServiceConfiguration() {
    @SuppressWarnings({"unchecked", "rawtypes"})
    CacheConfiguration<?, ?> cacheConfig = newCacheConfigurationBuilder(Description.class, Person.class, heap(10))
      .withService(new DefaultSerializerConfiguration(TestSerializer3.class, DefaultSerializerConfiguration.Type.KEY))
      .withService(new DefaultSerializerConfiguration(TestSerializer4.class, DefaultSerializerConfiguration.Type.VALUE))
      .build();

    CacheType cacheType = new CacheType();
    CacheEntryType keyType = new CacheEntryType();
    keyType.setValue("foo");
    cacheType.setKeyType(keyType);
    CacheEntryType valueType = new CacheEntryType();
    valueType.setValue("bar");
    cacheType.setValueType(valueType);

    cacheType = new DefaultSerializerConfigurationParser().unparseServiceConfiguration(cacheConfig, cacheType);

    assertThat(cacheType.getKeyType().getSerializer()).isEqualTo(TestSerializer3.class.getName());
    assertThat(cacheType.getValueType().getSerializer()).isEqualTo(TestSerializer4.class.getName());
  }

  @Test
  public void unparseServiceConfigurationWithInstance() {
    TestSerializer3<Integer> testSerializer3 = new TestSerializer3<>(ClassLoading.getDefaultClassLoader());
    TestSerializer4<Integer> testSerializer4 = new TestSerializer4<>(ClassLoading.getDefaultClassLoader());

    DefaultSerializerConfiguration<Integer> config1 = new DefaultSerializerConfiguration<>(testSerializer3, DefaultSerializerConfiguration.Type.KEY);
    DefaultSerializerConfiguration<Integer> config2 = new DefaultSerializerConfiguration<>(testSerializer4, DefaultSerializerConfiguration.Type.VALUE);
    CacheConfiguration<?, ?> cacheConfig = newCacheConfigurationBuilder(Description.class, Person.class, heap(10))
      .withService(config1).withService(config2).build();

    CacheType cacheType = new CacheType();
    CacheEntryType keyType = new CacheEntryType();
    keyType.setValue("foo");
    cacheType.setKeyType(keyType);
    CacheEntryType valueType = new CacheEntryType();
    valueType.setValue("bar");
    cacheType.setValueType(valueType);
    assertThatExceptionOfType(XmlConfigurationException.class).isThrownBy(() ->
      new DefaultSerializerConfigurationParser().unparseServiceConfiguration(cacheConfig, cacheType))
      .withMessage("%s", "XML translation for instance based initialization for " +
                         "DefaultSerializerConfiguration is not supported");
  }
}
