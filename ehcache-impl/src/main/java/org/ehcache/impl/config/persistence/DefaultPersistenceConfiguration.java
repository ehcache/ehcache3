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

package org.ehcache.impl.config.persistence;

import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.io.File;

/**
 * {@link ServiceCreationConfiguration} for the default {@link LocalPersistenceService}.
 */
public class DefaultPersistenceConfiguration implements ServiceCreationConfiguration<LocalPersistenceService, File> {

  private final File rootDirectory;

  /**
   * Creates a new configuration object with the provided parameters.
   *
   * @param rootDirectory the root directory to use for local persistence
   */
  public DefaultPersistenceConfiguration(File rootDirectory) {
    this.rootDirectory = rootDirectory;
  }

  /**
   * Returns the root directory to use for local persistence.
   *
   * @return the root directory
   */
  public File getRootDirectory() {
    return rootDirectory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<LocalPersistenceService> getServiceType() {
    return LocalPersistenceService.class;
  }

  @Override
  public File derive() {
    return getRootDirectory();
  }

  @Override
  public DefaultPersistenceConfiguration build(File file) {
    return new DefaultPersistenceConfiguration(file);
  }
}
