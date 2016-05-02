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

package org.ehcache.jsr107.internal;

import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.jsr107.config.Jsr107Service;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;

public class DefaultJsr107Service implements Jsr107Service {

  private final Jsr107Configuration configuration;

  public DefaultJsr107Service(Jsr107Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(final ServiceProvider<Service> serviceProvider) {
    // no-op
  }

  @Override
  public String getTemplateNameForCache(String cacheAlias) {
    final Jsr107Configuration cfg = configuration;
    if (cfg == null) {
      return null;
    }
    String template = cfg.getTemplates().get(cacheAlias);
    if (template != null) {
      return template;
    }
    return cfg.getDefaultTemplate();
  }

  @Override
  public void stop() {
    // no-op
  }

  @Override
  public boolean jsr107CompliantAtomics() {
    final Jsr107Configuration cfg = configuration;
    if (cfg == null) {
      return true;
    }
    return cfg.isJsr107CompliantAtomics();
  }

  @Override
  public ConfigurationElementState isManagementEnabledOnAllCaches() {
    if (configuration == null) {
      return null;
    } else {
      return configuration.isEnableManagementAll();
    }
  }

  @Override
  public ConfigurationElementState isStatisticsEnabledOnAllCaches() {
    if (configuration == null) {
      return null;
    } else {
      return configuration.isEnableStatisticsAll();
    }
  }
}
