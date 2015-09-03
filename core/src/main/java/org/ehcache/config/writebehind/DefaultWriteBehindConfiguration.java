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

import static java.lang.String.format;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public class DefaultWriteBehindConfiguration implements WriteBehindConfiguration {

  private int minWriteDelay = 0;
  private int maxWriteDelay = Integer.MAX_VALUE;
  private int rateLimitPerSecond = Integer.MAX_VALUE;
  private boolean writeCoalescing = false;
  private int writeBatchSize = 1;
  private int retryAttempts = 1;
  private int retryAttemptDelaySeconds = 1;
  private int writeBehindConcurrency = 1;
  private int writeBehindMaxQueueSize = Integer.MAX_VALUE;
  
  public DefaultWriteBehindConfiguration() {
  }
  
  @Override
  public int getMinWriteDelay() {
    return minWriteDelay;
  }
  
  @Override
  public int getMaxWriteDelay() {
    return maxWriteDelay;
  }
  
  @Override
  public int getRateLimitPerSecond() {
    return rateLimitPerSecond;
  }
  
  @Override
  public boolean isWriteCoalescing() {
    return writeCoalescing;
  }
  
  @Override
  public int getWriteBatchSize() {
    return writeBatchSize;
  }
  
  @Override
  public int getRetryAttempts() {
    return retryAttempts;
  }
  
  @Override
  public int getRetryAttemptDelaySeconds() {
    return retryAttemptDelaySeconds;
  }
  
  @Override
  public int getWriteBehindConcurrency() {
    return writeBehindConcurrency;
  }
  
  @Override
  public int getWriteBehindMaxQueueSize() {
    return writeBehindMaxQueueSize;
  }

  public void setMinWriteDelay(int minWriteDelay) {
    if (minWriteDelay < 0) {
      throw new IllegalArgumentException("Minimum write delay seconds cannot be less than 0");
    } else if (minWriteDelay > maxWriteDelay) {
      throw new IllegalArgumentException("Minimum write delay (" + minWriteDelay +
                                         "must be smaller than or equal to maximum write delay (" + maxWriteDelay + ")");
    }
    this.minWriteDelay = minWriteDelay;
  }

  public void setMaxWriteDelay(int maxWriteDelay) {
    if (maxWriteDelay < 0) {
      throw new IllegalArgumentException("Maximum write delay cannot be less than 1");
    } else if (maxWriteDelay < minWriteDelay) {
      throw new IllegalArgumentException("Maximum write delay (" + maxWriteDelay +
                                         ") must be larger than or equal to minimum write delay (" + minWriteDelay + ")");
    }
    this.maxWriteDelay = maxWriteDelay;
  }

  public void setRateLimitPerSecond(int rateLimitPerSecond) {
    if(rateLimitPerSecond < 1) {
      throw new IllegalArgumentException("RateLimitPerSecond cannot be less than 1.");
    }
    this.rateLimitPerSecond = rateLimitPerSecond;
  }

  public void setWriteCoalescing(boolean writeCoalescing) {
    this.writeCoalescing = writeCoalescing;
  }

  public void setWriteBatchSize(int writeBatchSize) {
    if(writeBatchSize < 1) {
      throw new IllegalArgumentException("Batchsize cannot be less than 1.");
    }
    this.writeBatchSize = writeBatchSize;
  }

  public void setRetryAttempts(int retryAttempts) {
    if(retryAttempts < 0) {
      throw new IllegalArgumentException("RetryAttempts cannot be less than 0.");
    }
    this.retryAttempts = retryAttempts;
  }

  public void setRetryAttemptDelaySeconds(int retryAttemptDelaySeconds) {
    if(retryAttemptDelaySeconds < 1) {
      throw new IllegalArgumentException("RetryAttemptDelaySeconds cannot be less than 1.");
    }
    this.retryAttemptDelaySeconds = retryAttemptDelaySeconds;
  }

  public void setWriteBehindConcurrency(int writeBehindConcurrency) {
    if(writeBehindConcurrency < 1) {
      throw new IllegalArgumentException("Concurrency Level cannot be less than 1.");
    }
    this.writeBehindConcurrency = writeBehindConcurrency;
  }

  public void setWriteBehindMaxQueueSize(int writeBehindMaxQueueSize) {
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
