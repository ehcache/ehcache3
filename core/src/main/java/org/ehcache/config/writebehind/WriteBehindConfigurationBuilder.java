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

/**
 * @author Abhilash
 *
 */
public class WriteBehindConfigurationBuilder {
  
  private int minWriteDelay = 1;
  private int maxWriteDelay = 1;
  private int rateLimitPerSecond = 0;
  private boolean writeCoalescing = false;
  private boolean writeBatching = false;
  private int writeBatchSize =1;
  private int retryAttempts = 0;
  private int retryAttemptDelaySeconds = 1;
  private int writeBehindConcurrency = 1;
  private int writeBehindMaxQueueSize = 0;
  
  
  private WriteBehindConfigurationBuilder() {
  }

  public static WriteBehindConfigurationBuilder newWriteBehindConfigurationBuilder() {
    return new WriteBehindConfigurationBuilder();
  }
  
  public WriteBehindConfiguration build() {
    return new WriteBehindConfiguration(minWriteDelay, maxWriteDelay, rateLimitPerSecond,
        writeCoalescing, writeBatching, writeBatchSize, retryAttempts,
        retryAttemptDelaySeconds, writeBehindConcurrency, writeBehindMaxQueueSize);
  }
  
  public WriteBehindConfigurationBuilder with(boolean coalesce, int concurrency, int size) {
    this.writeCoalescing = coalesce;
    this.writeBehindConcurrency = concurrency;
    this.writeBehindMaxQueueSize = size;
    
    return this;
  }
  
  public WriteBehindConfigurationBuilder withBatching(int batchSize) {
    this.writeBatching = true;
    this.writeBatchSize = batchSize;
    
    return this;
  }
  
  public WriteBehindConfigurationBuilder withRetryAttemps(int retryAttempts, int retryAttemptDelaySeconds) {
    this.retryAttempts = retryAttempts;
    this.retryAttemptDelaySeconds = retryAttemptDelaySeconds;
    
    return this;
  }
  
  public WriteBehindConfigurationBuilder withRateLimitPerSecond(int rateLimitPerSecond) {
    this.rateLimitPerSecond = rateLimitPerSecond;
    
    return this;
  }
  
  public WriteBehindConfigurationBuilder withWriteDelay(int minWriteDelay, int maxWriteDelay) {
    this.maxWriteDelay = maxWriteDelay;
    this.minWriteDelay = minWriteDelay;
    
    return this;
  }
}
