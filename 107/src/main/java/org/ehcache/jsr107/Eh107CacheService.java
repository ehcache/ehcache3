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

import org.ehcache.config.service.EhcacheService;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Ludovic Orban
 */
public class Eh107CacheService implements EhcacheService {

  private final boolean jsr107CompliantAtomics;

  public Eh107CacheService(boolean jsr107CompliantAtomics) {
    this.jsr107CompliantAtomics = jsr107CompliantAtomics;
  }

  @Override
  public boolean jsr107CompliantAtomics() {
    return jsr107CompliantAtomics;
  }

  @Override
  public void start(ServiceConfiguration<?> config) {
  }

  @Override
  public void stop() {
  }
}
