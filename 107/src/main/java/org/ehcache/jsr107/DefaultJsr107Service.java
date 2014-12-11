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
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class DefaultJsr107Service implements Jsr107Service {

  private volatile Jsr107Configuration configuration;

  @Override
  public void start(final ServiceConfiguration<?> serviceConfiguration) {
    configuration = (Jsr107Configuration) serviceConfiguration;
  }

  @Override
  public String getDefaultTemplate() {
    final Jsr107Configuration cfg = configuration;
    if (cfg == null) {
      return null;
    }
    return cfg.getDefaultTemplate();
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
    return null;
  }

  @Override
  public void stop() {
    configuration = null;
  }
}
