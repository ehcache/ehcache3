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
package org.ehcache.loaderwriter.writebehind;

import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;

/**
 * @author Abhilash
 *
 */
public class WriteBehindConfig {
  
  /**
   * Default minimum write delay
   */
  public static final int DEFAULT_MIN_WRITE_DELAY = 1;
  /**
   * Default maximum write delay
   */
  public static final int DEFAULT_MAX_WRITE_DELAY = 1;
  /**
   * Default rate limit per second
   */
  public static final int DEFAULT_RATE_LIMIT_PER_SECOND = 0;
  /**
   * Default write coalescing behavior
   */
  public static final boolean DEFAULT_WRITE_COALESCING = false;
  /**
   * Default writeBatching behavior
   */
  public static final boolean DEFAULT_WRITE_BATCHING = false;
  /**
   * Default write batch size
   */
  public static final int DEFAULT_WRITE_BATCH_SIZE = 1;
  /**
   * Default retry attempts
   */
  public static final int DEFAULT_RETRY_ATTEMPTS = 0;
  /**
   * Default retry attempt delay
   */
  public static final int DEFAULT_RETRY_ATTEMPT_DELAY_SECONDS = 1;

  /**
   * Default concurrency level for write behind
   */
  public static final int DEFAULT_WRITE_BEHIND_CONCURRENCY = 1;

  /**
   * Default max queue size for write behind
   */
  public static final int DEFAULT_WRITE_BEHIND_MAX_QUEUE_SIZE = 0;

  private boolean iswriteBehind = false;
  private int minWriteDelay = DEFAULT_MIN_WRITE_DELAY;
  private int maxWriteDelay = DEFAULT_MAX_WRITE_DELAY;
  private int rateLimitPerSecond = DEFAULT_RATE_LIMIT_PER_SECOND;
  private boolean writeCoalescing = DEFAULT_WRITE_COALESCING;
  private boolean writeBatching = DEFAULT_WRITE_BATCHING;
  private int writeBatchSize = DEFAULT_WRITE_BATCH_SIZE;
  private int retryAttempts = DEFAULT_RETRY_ATTEMPTS;
  private int retryAttemptDelaySeconds = DEFAULT_RETRY_ATTEMPT_DELAY_SECONDS;
  private int writeBehindConcurrency = DEFAULT_WRITE_BEHIND_CONCURRENCY;
  private int writeBehindMaxQueueSize = DEFAULT_WRITE_BEHIND_MAX_QUEUE_SIZE;
  
  private String alias;
  
  public WriteBehindConfig getWriteBehindConfig(CacheLoaderWriterConfiguration cacheLoaderWriterConfiguration) {
    WriteBehindConfig config = new WriteBehindConfig();
    // TODO get Alias
    config.setAlias("");
    config.setMaxWriteDelay(cacheLoaderWriterConfiguration.maxWriteDelay());
    config.setMinWriteDelay(cacheLoaderWriterConfiguration.minWriteDelay());
    config.setRateLimitPerSecond(cacheLoaderWriterConfiguration.rateLimitPerSecond());
    config.setRetryAttempts(cacheLoaderWriterConfiguration.retryAttempts());
    config.setRetryAttemptDelaySeconds(cacheLoaderWriterConfiguration.retryAttemptDelaySeconds());
    config.setWriteBatching(cacheLoaderWriterConfiguration.writeBatching());
    config.setWriteBatchSize(cacheLoaderWriterConfiguration.writeBatchSize());
    config.setWriteBehindConcurrency(cacheLoaderWriterConfiguration.writeBehindConcurrency());
    config.setWriteBehindMaxQueueSize(cacheLoaderWriterConfiguration.writeBehindMaxQueueSize());
    config.setWriteCoalescing(cacheLoaderWriterConfiguration.writeCoalescing());
    config.setWriteMode(cacheLoaderWriterConfiguration.isWriteBehind());
    return config;
  }


  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public void setWriteMode(boolean isWriteBehind) {
      this.iswriteBehind = isWriteBehind;
  }

  /**
   * Get the write mode in terms of the mode enum
   */
  public boolean getWriteMode() {
      return this.iswriteBehind;
  }

  /**
   * Set the minimum number of seconds to wait before writing behind. If set to a value greater than 0, it permits
   * operations to build up in the queue. This is different from the maximum write delay in that by waiting a minimum
   * amount of time, work is always being built up. If the minimum write delay is set to zero and the {@code CacheWriter}
   * performs its work very quickly, the overhead of processing the write behind queue items becomes very noticeable
   * in a cluster since all the operations might be done for individual items instead of for a collection of them.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_MIN_WRITE_DELAY}).
   *
   * @param minWriteDelay the minimum number of seconds to wait before writing behind
   */
  public void setMinWriteDelay(int minWriteDelay) {
          this.minWriteDelay = minWriteDelay;
  }

  /**
   * Get the minimum number of seconds to wait before writing behind
   */
  public int getMinWriteDelay() {
      return this.minWriteDelay;
  }

  /**
   * Set the maximum number of seconds to wait before writing behind. If set to a value greater than 0, it permits
   * operations to build up in the queue to enable effective coalescing and batching optimisations.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_MAX_WRITE_DELAY}).
   *
   * @param maxWriteDelay the maximum number of seconds to wait before writing behind
   */
  public void setMaxWriteDelay(int maxWriteDelay) {
          this.maxWriteDelay = maxWriteDelay;
  }

  /**
   * Get the maximum number of seconds to wait before writing behind
   */
  public int getMaxWriteDelay() {
      return this.maxWriteDelay;
  }

  /**
   * Sets the maximum number of write operations to allow per second when {@link #writeBatching} is enabled.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_RATE_LIMIT_PER_SECOND}.
   *
   * @param rateLimitPerSecond the number of write operations to allow; use a number {@code &lt;=0} to disable rate limiting.
   */
  public void setRateLimitPerSecond(int rateLimitPerSecond) {
          this.rateLimitPerSecond = rateLimitPerSecond;
  }

  /**
   * Get the maximum number of write operations to allow per second.
   */
  public int getRateLimitPerSecond() {
      return rateLimitPerSecond;
  }

  /**
   * Sets whether to use write coalescing. If set to {@code true} and multiple operations on the same key are present
   * in the write-behind queue, only the latest write is done, as the others are redundant. This can dramatically
   * reduce load on the underlying resource.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_WRITE_COALESCING}.
   *
   * @param writeCoalescing {@code true} to enable write coalescing; or {@code false} to disable it
   */
  public void setWriteCoalescing(boolean writeCoalescing) {
      this.writeCoalescing = writeCoalescing;
  }

  /**
   * @return this configuration instance
   * @see #setWriteCoalescing(boolean)
   */
  public boolean getWriteCoalescing() {
      return writeCoalescing;
  }

  /**
   * Sets whether to batch write operations. If set to {@code true}, {@link net.sf.ehcache.writer.CacheWriter#writeAll} and {@code CacheWriter#deleteAll}
   * will be called rather than {@link net.sf.ehcache.writer.CacheWriter#write} and {@link net.sf.ehcache.writer.CacheWriter#delete} being called for each key. Resources such
   * as databases can perform more efficiently if updates are batched, thus reducing load.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_WRITE_BATCHING}.
   *
   * @param writeBatching {@code true} if write operations should be batched; {@code false} otherwise
   */
  public void setWriteBatching(boolean writeBatching) {
      this.writeBatching = writeBatching;
  }

  /**
   * Check whether write operations should be batched
   */
  public boolean getWriteBatching() {
      return this.writeBatching;
  }

  /**
   * Sets the number of operations to include in each batch when {@link #writeBatching} is enabled. If there are less
   * entries in the write-behind queue than the batch size, the queue length size is used.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_WRITE_BATCH_SIZE}.
   *
   * @param writeBatchSize the number of operations to include in each batch; numbers smaller than {@code 1} will cause
   *                       the default batch size to be used
   */
  public void setWriteBatchSize(int writeBatchSize) {
          this.writeBatchSize = writeBatchSize;
  }

  /**
   * @return this configuration instance
   * @see #setWriteBatchSize(int)
   */
  public WriteBehindConfig writeBatchSize(int writeBatchSize) {
      setWriteBatchSize(writeBatchSize);
      return this;
  }

  /**
   * Retrieves the size of the batch operation.
   */
  public int getWriteBatchSize() {
      return writeBatchSize;
  }

  /**
   * Sets the number of times the operation is retried in the {@code CacheWriter}, this happens after the
   * original operation.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_RETRY_ATTEMPTS}.
   *
   * @param retryAttempts the number of retries for a particular element
   */
  public void setRetryAttempts(int retryAttempts) {
          this.retryAttempts = retryAttempts;
  }

  /**
   * Retrieves the number of times the write of element is retried.
   */
  public int getRetryAttempts() {
      return retryAttempts;
  }

  /**
   * Sets the number of seconds to wait before retrying an failed operation.
   * <p/>
   * This is only applicable to write behind mode.
   * <p/>
   * Defaults to {@value #DEFAULT_RETRY_ATTEMPT_DELAY_SECONDS}.
   *
   * @param retryAttemptDelaySeconds the number of seconds to wait before retrying an operation
   */
  public void setRetryAttemptDelaySeconds(int retryAttemptDelaySeconds) {
          this.retryAttemptDelaySeconds = retryAttemptDelaySeconds;
  }

  /**
   * Retrieves the number of seconds to wait before retrying an failed operation.
   */
  public int getRetryAttemptDelaySeconds() {
      return retryAttemptDelaySeconds;
  }

  /**
   * Configures the amount of thread/bucket pairs WriteBehind should use
   * @param concurrency Amount of thread/bucket pairs, has to be at least 1
   */
  public void setWriteBehindConcurrency(int concurrency) {
          this.writeBehindConcurrency = concurrency;
  }

  /**
   * Accessor
   * @return the amount of bucket/thread pairs configured for this cache's write behind
   */
  public int getWriteBehindConcurrency() {
      return writeBehindConcurrency;
  }

  /**
   * Configures the maximum amount of operations to be on the waiting queue, before it blocks
   * @param writeBehindMaxQueueSize maximum amount of operations allowed on the waiting queue
   */
  public void setWriteBehindMaxQueueSize(final int writeBehindMaxQueueSize) {
          this.writeBehindMaxQueueSize = writeBehindMaxQueueSize;
  }

  /**
   * Accessor
   * @return the maximum amount of operations allowed on the write behind queue
   */
  public int getWriteBehindMaxQueueSize() {
      return writeBehindMaxQueueSize;
  }

  /**
   * Overrided hashCode()
   */
  @Override
  public int hashCode() {
      final int prime = 31;
      final int primeTwo = 1231;
      final int primeThree = 1237;
      int result = 1;
      result = prime * result + maxWriteDelay;
      result = prime * result + minWriteDelay;
      result = prime * result + rateLimitPerSecond;
      result = prime * result + retryAttemptDelaySeconds;
      result = prime * result + retryAttempts;
      result = prime * result + writeBatchSize;
      result = prime * result + (writeBatching ? primeTwo : primeThree);
      result = prime * result + (writeCoalescing ? primeTwo : primeThree);
      result = prime * result + (iswriteBehind ? primeTwo : primeThree);;
      result = prime * result + writeBehindConcurrency;
      return result;
  }

  /**
   * Override equals()
   */
  @Override
  public boolean equals(Object obj) {
      if (this == obj) {
          return true;
      }
      if (obj == null) {
          return false;
      }
      if (getClass() != obj.getClass()) {
          return false;
      }
      WriteBehindConfig other = (WriteBehindConfig) obj;
      if (maxWriteDelay != other.maxWriteDelay) {
          return false;
      }
      if (minWriteDelay != other.minWriteDelay) {
          return false;
      }
      if (rateLimitPerSecond != other.rateLimitPerSecond) {
          return false;
      }
      if (retryAttemptDelaySeconds != other.retryAttemptDelaySeconds) {
          return false;
      }
      if (retryAttempts != other.retryAttempts) {
          return false;
      }
      if (writeBatchSize != other.writeBatchSize) {
          return false;
      }
      if (writeBatching != other.writeBatching) {
          return false;
      }
      if (writeCoalescing != other.writeCoalescing) {
          return false;
      }
      if (writeBehindConcurrency != other.writeBehindConcurrency) {
          return false;
      }
      if (iswriteBehind != other.iswriteBehind) {
        return false;
    }
      return true;
  }


}
