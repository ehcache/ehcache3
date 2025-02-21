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

package org.ehcache.xml.provider;

import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.MemoryType;
import org.ehcache.xml.model.MemoryUnit;
import org.ehcache.xml.model.SizeOfEngineLimits;
import org.ehcache.xml.model.SizeofType;

import java.math.BigInteger;

@Deprecated
public class DefaultSizeOfEngineProviderConfigurationParser
  extends SimpleCoreServiceCreationConfigurationParser<ConfigType, SizeofType, org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration> {

  public DefaultSizeOfEngineProviderConfigurationParser() {
    super(org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration.class,
      ConfigType::getHeapStore, ConfigType::setHeapStore,
      config -> {
        SizeOfEngineLimits sizeOfEngineLimits = new SizeOfEngineLimits(config);
        return new org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration(sizeOfEngineLimits.getMaxObjectSize(),
          sizeOfEngineLimits.getUnit(), sizeOfEngineLimits.getMaxObjectGraphSize());
      },
      config -> new SizeofType()
        .withMaxObjectGraphSize(new SizeofType.MaxObjectGraphSize().withValue(BigInteger.valueOf(config.getMaxObjectGraphSize())))
        .withMaxObjectSize(new MemoryType()
          .withValue(BigInteger.valueOf(config.getMaxObjectSize()))
          .withUnit(MemoryUnit.fromValue(config.getUnit().toString()))
        )
    );
  }
}
