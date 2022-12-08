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

import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.CopierType;

import static java.util.stream.Collectors.toList;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultCopyProviderConfigurationParser
  extends SimpleCoreServiceCreationConfigurationParser<CopierType, DefaultCopyProviderConfiguration> {

  @SuppressWarnings("unchecked")
  public DefaultCopyProviderConfigurationParser() {
    super(DefaultCopyProviderConfiguration.class,
      ConfigType::getDefaultCopiers, ConfigType::setDefaultCopiers,
      (config, loader) -> {
        DefaultCopyProviderConfiguration configuration = new DefaultCopyProviderConfiguration();
        for (CopierType.Copier copier : config.getCopier()) {
          configuration.addCopierFor(getClassForName(copier.getType(), loader), (Class) getClassForName(copier.getValue(), loader));
        }
        return configuration;
      },
      config -> new CopierType()
        .withCopier(config.getDefaults().entrySet().stream().map(entry -> new CopierType.Copier()
          .withType(entry.getKey().getName())
          .withValue(entry.getValue().getClazz().getName())).collect(toList()))
    );
  }
}
