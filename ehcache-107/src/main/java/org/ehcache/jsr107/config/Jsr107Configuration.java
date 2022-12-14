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

package org.ehcache.jsr107.config;

import org.ehcache.jsr107.Jsr107Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link ServiceCreationConfiguration} for default {@link Jsr107Service} implementation.
 */
public class Jsr107Configuration implements ServiceCreationConfiguration<Jsr107Service, Void> {

  private final String defaultTemplate;
  private final boolean jsr107CompliantAtomics;
  private final ConfigurationElementState enableManagementAll;
  private final ConfigurationElementState enableStatisticsAll;
  private final Map<String, String> templates;

  /**
   * Creates a new configuration with the provided parameters.
   *  @param defaultTemplate the default template
   * @param templates cache alias to template name map
   * @param jsr107CompliantAtomics behaviour of loader writer in atomic operations
   * @param enableManagementAll enable management JMX
   * @param enableStatisticsAll enable statistics JMX
   */
  public Jsr107Configuration(final String defaultTemplate, final Map<String, String> templates,
                             boolean jsr107CompliantAtomics, ConfigurationElementState enableManagementAll, ConfigurationElementState enableStatisticsAll) {
    this.defaultTemplate = defaultTemplate;
    this.jsr107CompliantAtomics = jsr107CompliantAtomics;
    this.enableManagementAll = enableManagementAll;
    this.enableStatisticsAll = enableStatisticsAll;
    this.templates = new ConcurrentHashMap<>(templates);
  }

  /**
   * Returns the default template, or {@code null} if not configured.
   *
   * @return the default template or {@code null}
   */
  public String getDefaultTemplate() {
    return defaultTemplate;
  }

  /**
   * Returns the cache alias to template name map.
   *
   * @return the cache alias to template name map
   */
  public Map<String, String> getTemplates() {
    return templates;
  }

  /**
   * Indicates loader writer behaviour in atomic methods.
   * <p>
   * If {@code true} then loader writer will <em>NOT</em> be used in atomic methods, if {@code false} it will be.
   *
   * @return {@code true} or {@code false} depending on configuration
   */
  public boolean isJsr107CompliantAtomics() {
    return jsr107CompliantAtomics;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<Jsr107Service> getServiceType() {
    return Jsr107Service.class;
  }

  /**
   * Indicates if all created caches should have management enabled.
   *
   * @return {@code true} to enable management on all caches, {@code false} otherwise
   */
  public ConfigurationElementState isEnableManagementAll() {
    return enableManagementAll;
  }

  /**
   * Indicates if all created caches should have statistics enabled.
   *
   * @return {@code true} to enable management on all caches, {@code false} otherwise
   */
  public ConfigurationElementState isEnableStatisticsAll() {
    return enableStatisticsAll;
  }
}
