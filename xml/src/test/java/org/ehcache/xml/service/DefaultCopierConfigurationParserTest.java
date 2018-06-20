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
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.model.CacheEntryType;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.copier.AnotherPersonCopier;
import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.DescriptionCopier;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.copier.PersonCopier;
import com.pany.ehcache.integration.TestCacheLoaderWriter;

import java.io.IOException;
import java.util.Collection;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;

public class DefaultCopierConfigurationParserTest extends ServiceConfigurationParserTestBase {

  public DefaultCopierConfigurationParserTest() {
    super(new DefaultCopierConfigurationParser());
  }

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = getCacheDefinitionFrom("/configs/cache-copiers.xml", "baz");

    @SuppressWarnings("rawtypes")
    Collection<DefaultCopierConfiguration> copierConfigs =
      findAmongst(DefaultCopierConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfigs).hasSize(2);

    for(DefaultCopierConfiguration<?> copierConfig : copierConfigs) {
      if(copierConfig.getType() == DefaultCopierConfiguration.Type.KEY) {
        assertThat(copierConfig.getClazz()).isEqualTo(SerializingCopier.class);
      } else {
        assertThat(copierConfig.getClazz()).isEqualTo(AnotherPersonCopier.class);
      }
    }
  }

  @Test
  public void unparseServiceConfiguration() {
    @SuppressWarnings({"unchecked", "rawtypes"})
    CacheConfiguration<?, ?> cacheConfig = newCacheConfigurationBuilder(Description.class, Person.class, heap(10))
      .add(new DefaultCopierConfiguration<>(DescriptionCopier.class, DefaultCopierConfiguration.Type.KEY))
      .add(new DefaultCopierConfiguration<>(PersonCopier.class, DefaultCopierConfiguration.Type.VALUE))
      .build();

    CacheType cacheType = new CacheType();
    CacheEntryType keyType = new CacheEntryType();
    keyType.setValue("foo");
    cacheType.setKeyType(keyType);
    CacheEntryType valueType = new CacheEntryType();
    valueType.setValue("bar");
    cacheType.setValueType(valueType);

    cacheType = parser.unparseServiceConfiguration(cacheConfig, cacheType);

    assertThat(cacheType.getKeyType().getCopier()).isEqualTo(DescriptionCopier.class.getName());
    assertThat(cacheType.getValueType().getCopier()).isEqualTo(PersonCopier.class.getName());
  }
}
