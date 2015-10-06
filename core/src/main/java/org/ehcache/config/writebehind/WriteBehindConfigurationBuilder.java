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

import org.ehcache.config.Builder;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;

/**
 * @author Abhilash
 *
 */
public class WriteBehindConfigurationBuilder implements Builder<WriteBehindConfiguration> {
  
  private Integer minWriteDelay;
  private Integer maxWriteDelay;
  private Boolean writeCoalescing;
  private Integer writeBatchSize;
  private Integer writeBehindConcurrency;
  private Integer writeBehindMaxQueueSize;
  
  
  private WriteBehindConfigurationBuilder() {
  }

  private WriteBehindConfigurationBuilder(WriteBehindConfigurationBuilder other) {
    minWriteDelay = other.minWriteDelay;
    maxWriteDelay = other.maxWriteDelay;
    writeCoalescing = other.writeCoalescing;
    writeBatchSize = other.writeBatchSize;
    writeBehindConcurrency = other.writeBehindConcurrency;
    writeBehindMaxQueueSize = other.writeBehindMaxQueueSize;
  }

  public static WriteBehindConfigurationBuilder newWriteBehindConfiguration() {
    return new WriteBehindConfigurationBuilder();
  }
  
  public WriteBehindConfiguration build() {
    DefaultWriteBehindConfiguration configuration = new DefaultWriteBehindConfiguration();
    if (minWriteDelay != null) {
      configuration.setMinWriteDelay(minWriteDelay);
    }
    if (maxWriteDelay != null) {
      configuration.setMaxWriteDelay(maxWriteDelay);
    }
    if (writeCoalescing != null) {
      configuration.setWriteCoalescing(writeCoalescing);
    }
    if (writeBatchSize != null) {
      configuration.setWriteBatchSize(writeBatchSize);
    }
    if (writeBehindConcurrency != null) {
      configuration.setWriteBehindConcurrency(writeBehindConcurrency);
    }
    if (writeBehindMaxQueueSize != null) {
      configuration.setWriteBehindMaxQueueSize(writeBehindMaxQueueSize);
    }
    return configuration;
  }
  
  public WriteBehindConfigurationBuilder queueSize(int size) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.writeBehindMaxQueueSize = size;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder concurrencyLevel(int concurrency) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.writeBehindConcurrency = concurrency;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder enableCoalescing() {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.writeCoalescing = true;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder disableCoalescing() {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.writeCoalescing = false;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder batchSize(int batchSize) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.writeBatchSize = batchSize;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder delay(int minWriteDelay, int maxWriteDelay) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.maxWriteDelay = maxWriteDelay;
    otherBuilder.minWriteDelay = minWriteDelay;
    return otherBuilder;
  }
}
