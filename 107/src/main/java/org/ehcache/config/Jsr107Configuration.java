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

package org.ehcache.config;

import org.ehcache.jsr107.Jsr107Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Alex Snaps
 */
public class Jsr107Configuration implements ServiceCreationConfiguration<Jsr107Service> {

  private final String defaultTemplate;
  private final boolean jsr107CompliantAtomics;
  private final Map<String, String> templates;

  public Jsr107Configuration(final String defaultTemplate, final Map<String, String> templates, boolean jsr107CompliantAtomics) {
    this.defaultTemplate = defaultTemplate;
    this.jsr107CompliantAtomics = jsr107CompliantAtomics;
    this.templates = new ConcurrentHashMap<String, String>(templates);
  }

  public String getDefaultTemplate() {
    return defaultTemplate;
  }

  public Map<String, String> getTemplates() {
    return templates;
  }

  public boolean isJsr107CompliantAtomics() {
    return jsr107CompliantAtomics;
  }

  @Override
  public Class<Jsr107Service> getServiceType() {
    return Jsr107Service.class;
  }

}
