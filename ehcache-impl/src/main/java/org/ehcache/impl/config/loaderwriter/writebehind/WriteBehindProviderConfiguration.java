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

package org.ehcache.impl.config.loaderwriter.writebehind;

import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 *
 * @author cdennis
 */
public class WriteBehindProviderConfiguration implements ServiceCreationConfiguration<WriteBehindProvider, String> {

  private final String threadPoolAlias;

  public WriteBehindProviderConfiguration(String threadPoolAlias) {
    this.threadPoolAlias = threadPoolAlias;
  }

  public String getThreadPoolAlias() {
    return threadPoolAlias;
  }

  @Override
  public Class<WriteBehindProvider> getServiceType() {
    return WriteBehindProvider.class;
  }

  @Override
  public String derive() {
    return getThreadPoolAlias();
  }

  @Override
  public WriteBehindProviderConfiguration build(String alias) {
    return new WriteBehindProviderConfiguration(alias);
  }
}
