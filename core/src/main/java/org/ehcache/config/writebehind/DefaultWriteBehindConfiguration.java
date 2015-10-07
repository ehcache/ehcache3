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
package org.ehcache.config.writebehind;

import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindDecoratorLoaderWriterProvider;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public class DefaultWriteBehindConfiguration implements WriteBehindConfiguration {

  private final BatchingConfiguration batchingConfig;

  private int writeBehindConcurrency = 1;
  private int writeBehindMaxQueueSize = Integer.MAX_VALUE;
  
  public DefaultWriteBehindConfiguration(BatchingConfiguration batchingConfig) {
    this.batchingConfig = batchingConfig;
  }
  
  @Override
  public int getConcurrency() {
    return writeBehindConcurrency;
  }
  
  @Override
  public int getMaxQueueSize() {
    return writeBehindMaxQueueSize;
  }

  @Override
  public BatchingConfiguration getBatchingConfiguration() {
    return batchingConfig;
  }

  public void setConcurrency(int writeBehindConcurrency) {
    if(writeBehindConcurrency < 1) {
      throw new IllegalArgumentException("Concurrency Level cannot be less than 1.");
    }
    this.writeBehindConcurrency = writeBehindConcurrency;
  }

  public void setMaxQueueSize(int writeBehindMaxQueueSize) {
    if(writeBehindMaxQueueSize < 1) {
      throw new IllegalArgumentException("WriteBehind queue size cannot be less than 1.");
    }
    this.writeBehindMaxQueueSize = writeBehindMaxQueueSize;
  }

  @Override
  public Class<WriteBehindDecoratorLoaderWriterProvider> getServiceType() {
    return WriteBehindDecoratorLoaderWriterProvider.class;
  }
  
}
