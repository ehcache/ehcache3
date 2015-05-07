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

/**
 * @author Abhilash
 *
 */
public class WriteBehindConfigurationBuilder implements Builder<WriteBehindConfiguration> {
  
  private int minWriteDelay = 1;
  private int maxWriteDelay = 1;
  private int rateLimitPerSecond = 0;
  private boolean writeCoalescing = false;
  private int writeBatchSize =1;
  private int retryAttempts = 0;
  private int retryAttemptDelaySeconds = 1;
  private int writeBehindConcurrency = 1;
  private int writeBehindMaxQueueSize = 0;
  
  
  private WriteBehindConfigurationBuilder() {
  }

  private WriteBehindConfigurationBuilder(WriteBehindConfigurationBuilder other) {
    minWriteDelay = other.minWriteDelay;
    maxWriteDelay = other.maxWriteDelay;
    rateLimitPerSecond = other.rateLimitPerSecond;
    writeCoalescing = other.writeCoalescing;
    writeBatchSize = other.writeBatchSize;
    retryAttempts = other.retryAttempts;
    retryAttemptDelaySeconds = other.retryAttemptDelaySeconds;
    writeBehindConcurrency = other.writeBehindConcurrency;
    writeBehindMaxQueueSize = other.writeBehindMaxQueueSize;
  }

  public static WriteBehindConfigurationBuilder newWriteBehindConfiguration() {
    return new WriteBehindConfigurationBuilder();
  }
  
  public WriteBehindConfiguration build() {
    return new WriteBehindConfiguration(minWriteDelay, maxWriteDelay, rateLimitPerSecond,
        writeCoalescing, writeBatchSize, retryAttempts,
        retryAttemptDelaySeconds, writeBehindConcurrency, writeBehindMaxQueueSize);
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
  
  public WriteBehindConfigurationBuilder retry(int retryAttempts, int retryAttemptDelaySeconds) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.retryAttempts = retryAttempts;
    otherBuilder.retryAttemptDelaySeconds = retryAttemptDelaySeconds;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder rateLimit(int rateLimitPerSecond) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.rateLimitPerSecond = rateLimitPerSecond;
    return otherBuilder;
  }
  
  public WriteBehindConfigurationBuilder delay(int minWriteDelay, int maxWriteDelay) {
    WriteBehindConfigurationBuilder otherBuilder = new WriteBehindConfigurationBuilder(this);
    otherBuilder.maxWriteDelay = maxWriteDelay;
    otherBuilder.minWriteDelay = minWriteDelay;
    return otherBuilder;
  }
}
