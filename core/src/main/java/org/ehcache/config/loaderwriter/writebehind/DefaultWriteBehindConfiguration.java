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
package org.ehcache.config.loaderwriter.writebehind;

import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public class DefaultWriteBehindConfiguration implements WriteBehindConfiguration {

  private final BatchingConfiguration batchingConfig;
  private final int concurrency;
  private final int queueSize;
  private final String executorAlias;
  
  public DefaultWriteBehindConfiguration(String executorAlias, int concurrency, int queueSize, BatchingConfiguration batchingConfig) {
    this.concurrency = concurrency;
    this.queueSize = queueSize;
    this.executorAlias = executorAlias;
    this.batchingConfig = batchingConfig;
  }
  
  @Override
  public int getConcurrency() {
    return concurrency;
  }
  
  @Override
  public int getMaxQueueSize() {
    return queueSize;
  }

  @Override
  public String getThreadPoolAlias() {
    return executorAlias;
  }

  @Override
  public BatchingConfiguration getBatchingConfiguration() {
    return batchingConfig;
  }

  @Override
  public Class<WriteBehindProvider> getServiceType() {
    return WriteBehindProvider.class;
  }
  
}
