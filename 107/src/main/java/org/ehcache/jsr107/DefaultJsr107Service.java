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

package org.ehcache.jsr107;

import org.ehcache.config.Jsr107Configuration;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class DefaultJsr107Service implements Jsr107Service {

  private volatile Jsr107Configuration configuration;

  public DefaultJsr107Service(Jsr107Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start(final ServiceConfiguration<?> serviceConfiguration, final ServiceProvider serviceProvider) {
    if (configuration != null) {
      if (!configuration.equals(serviceConfiguration)) {
        throw new IllegalStateException("Trying to start service with different configuration");
      }
    } else {
      configuration = (Jsr107Configuration)serviceConfiguration;
    }
  }

  @Override
  public String getTemplateNameForCache(String name) {
    final Jsr107Configuration cfg = configuration;
    if (cfg == null) {
      return null;
    }
    String template = cfg.getTemplates().get(name);
    if (template != null) {
      return template;
    }
    return cfg.getDefaultTemplate();
  }

  @Override
  public void stop() {
    configuration = null;
  }

  @Override
  public boolean jsr107CompliantAtomics() {
    final Jsr107Configuration cfg = configuration;
    if (cfg == null) {
      return true;
    }
    return cfg.isJsr107CompliantAtomics();
  }
}
