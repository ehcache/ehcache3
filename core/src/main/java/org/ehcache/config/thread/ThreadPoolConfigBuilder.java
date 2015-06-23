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
package org.ehcache.config.thread;

import java.util.concurrent.TimeUnit;

import org.ehcache.config.Builder;
import org.ehcache.spi.service.ThreadPoolConfig;

public class ThreadPoolConfigBuilder implements Builder<ThreadPoolConfig> {

  private int corePoolSize;
  private int maximumThreads;
  private int keepAliveTime;
  private TimeUnit timeUnit;

  private ThreadPoolConfigBuilder() {
  }
  
  private ThreadPoolConfigBuilder(ThreadPoolConfigBuilder other) {
    this.corePoolSize = other.corePoolSize;
    this.maximumThreads = other.maximumThreads;
    this.keepAliveTime = other.keepAliveTime;
    this.timeUnit = other.timeUnit;
  }
  
  public static ThreadPoolConfigBuilder newThreadPoolConfigBuilder() {
    return new ThreadPoolConfigBuilder();
  }
  
  public ThreadPoolConfigBuilder corePoolSize(int corePoolSize) {
    ThreadPoolConfigBuilder newBuilder = new ThreadPoolConfigBuilder(this);
    newBuilder.corePoolSize = corePoolSize;
    return newBuilder;
  }
  
  public ThreadPoolConfigBuilder maximumThreads(int maximumThreads) {
    ThreadPoolConfigBuilder newBuilder = new ThreadPoolConfigBuilder(this);
    newBuilder.maximumThreads = maximumThreads;
    
    return newBuilder;
  }

  public ThreadPoolConfigBuilder keepAliveTime(int keepAliveTime) {
    ThreadPoolConfigBuilder newBuilder = new ThreadPoolConfigBuilder(this);
    newBuilder.keepAliveTime = keepAliveTime;
    return newBuilder;
  }
  
  public ThreadPoolConfigBuilder timeUnit(TimeUnit timeUnit) {
    ThreadPoolConfigBuilder newBuilder = new ThreadPoolConfigBuilder(this);
    newBuilder.timeUnit = timeUnit;
    return newBuilder;
  }
  
  
  @Override
  public ThreadPoolConfig build() {
    return null;
  }

}
