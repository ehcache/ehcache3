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
package org.ehcache.spi.loaderwriter;

import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Abhilash
 *
 */
public interface CacheLoaderWriterConfiguration extends ServiceConfiguration<CacheLoaderWriterFactory> {

  /**
   * the write mode in terms of the mode enum
   * 
   * @return Retrieves the write mode in terms of the mode enum
   */
  boolean isWriteBehind();
  
  /**
   * the minimum number of seconds to wait before writing behind
   * 
   * @return Retrieves the minimum number of seconds to wait before writing behind
   */
  int minWriteDelay();
  
  /**
   * the maximum number of seconds to wait before writing behind
   * 
   * @return Retrieves the maximum number of seconds to wait before writing behind
   */
  int maxWriteDelay();
  
  /**
   * the maximum number of write operations to allow per second.
   * 
   * @return Retrieves the maximum number of write operations to allow per second.
   */
  int rateLimitPerSecond();
  
  /**
   * write coalescing behavior
   * 
   * @return Retrieves the write coalescing behavior is enabled or not 
   */
  boolean writeCoalescing();
  
  /**
   * whether write operations should be batched
   * 
   * @return Retrieves whether write operations should be batched
   */
  boolean writeBatching();
  
  /**
   * the size of the batch operation.
   * 
   * @return Retrieves the size of the batch operation.
   */
  int writeBatchSize();
  
  /**
   * the number of times the write of element is retried.
   * 
   * @return Retrieves the number of times the write of element is retried.
   */
  int retryAttempts();
  
  /**
   * the number of seconds to wait before retrying an failed operation. 
   * 
   * @return Retrieves the number of seconds to wait before retrying an failed operation. 
   */
  int retryAttemptDelaySeconds();
  
  /**
   * the amount of bucket/thread pairs configured for this cache's write behind
   * 
   * @return Retrieves the amount of bucket/thread pairs configured for this cache's write behind
   */
  int writeBehindConcurrency();
  
  /**
   * the maximum amount of operations allowed on the write behind queue
   * 
   * @return Retrieves the maximum amount of operations allowed on the write behind queue
   */
  int writeBehindMaxQueueSize();
  
}
