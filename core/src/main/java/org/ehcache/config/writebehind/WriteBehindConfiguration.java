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

import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Abhilash
 *
 */
public class WriteBehindConfiguration implements ServiceConfiguration<WriteBehindDecoratorLoaderWriterProvider> {

  private int minWriteDelay;
  private int maxWriteDelay;
  private int rateLimitPerSecond;
  private boolean writeCoalescing;
  private boolean writeBatching;
  private int writeBatchSize;
  private int retryAttempts;
  private int retryAttemptDelaySeconds;
  private int writeBehindConcurrency;
  private int writeBehindMaxQueueSize;
  
  public WriteBehindConfiguration(int minWriteDelay, int maxWriteDelay, int rateLimitPerSecond, boolean writeCoalescing, 
      boolean writeBatching, int writeBatchSize, int retryAttempts,int retryAttemptDelaySeconds,
      int writeBehindConcurrency, int writeBehindMaxQueueSize) {
    
    this.minWriteDelay = minWriteDelay;
    this.maxWriteDelay = maxWriteDelay;
    this.rateLimitPerSecond = rateLimitPerSecond;
    this.writeCoalescing = writeCoalescing;
    this.writeBatching = writeBatching;
    this.writeBatchSize = writeBatchSize;
    this.retryAttempts = retryAttempts;
    this.retryAttemptDelaySeconds = retryAttemptDelaySeconds;
    this.writeBehindConcurrency = writeBehindConcurrency;
    this.writeBehindMaxQueueSize = writeBehindMaxQueueSize;
  }
  
  /**
   * the minimum number of seconds to wait before writing behind
   * 
   * @return Retrieves the minimum number of seconds to wait before writing behind
   */
  public int getMinWriteDelay() {
    return minWriteDelay;
  }
  
  /**
   * the maximum number of seconds to wait before writing behind
   * 
   * @return Retrieves the maximum number of seconds to wait before writing behind
   */
  public int getMaxWriteDelay() {
    return maxWriteDelay;
  }
  
  /**
   * the maximum number of write operations to allow per second.
   * 
   * @return Retrieves the maximum number of write operations to allow per second.
   */
  public int getRateLimitPerSecond() {
    return rateLimitPerSecond;
  }
  
  /**
   * write coalescing behavior
   * 
   * @return Retrieves the write coalescing behavior is enabled or not 
   */
  public boolean isWriteCoalescing() {
    return writeCoalescing;
  }
  
  /**
   * whether write operations should be batched
   * 
   * @return Retrieves whether write operations should be batched
   */
  public boolean isWriteBatching() {
    return writeBatching;
  }
  
  /**
   * the size of the batch operation.
   * 
   * @return Retrieves the size of the batch operation.
   */
  public int getWriteBatchSize() {
    return writeBatchSize;
  }
  
  /**
   * the number of times the write of element is retried.
   * 
   * @return Retrieves the number of times the write of element is retried.
   */
  public int getRetryAttempts() {
    return retryAttempts;
  }
  
  /**
   * the number of seconds to wait before retrying an failed operation. 
   * 
   * @return Retrieves the number of seconds to wait before retrying an failed operation. 
   */
  public int getRetryAttemptDelaySeconds() {
    return retryAttemptDelaySeconds;
  }
  
  /**
   * the amount of bucket/thread pairs configured for this cache's write behind
   * 
   * @return Retrieves the amount of bucket/thread pairs configured for this cache's write behind
   */
  public int getWriteBehindConcurrency() {
    return writeBehindConcurrency;
  }
  
  /**
   * the maximum amount of operations allowed on the write behind queue
   * 
   * @return Retrieves the maximum amount of operations allowed on the write behind queue
   */
  public int getWriteBehindMaxQueueSize() {
    return writeBehindMaxQueueSize;
  }

  @Override
  public Class<WriteBehindDecoratorLoaderWriterProvider> getServiceType() {
    return WriteBehindDecoratorLoaderWriterProvider.class;
  }
  
}
