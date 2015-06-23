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
package org.ehcache.spi.service;

import java.util.concurrent.TimeUnit;

/**
 * @author palmanojkumar
 *
 */
public class ThreadPoolConfig {
  
  private int corePoolSize;
  private int maximumThreads;
  private int keepAliveTime;
  private TimeUnit timeUnit;
  
  private ThreadFactoryConfig threadFactoryConfig;
  
  public ThreadPoolConfig() {
  }
  
  public ThreadPoolConfig(int corePoolSize, int maximumThreads, int keepAliveTime, TimeUnit timeUnit, ThreadFactoryConfig threadFactoryConfig) {
    this.corePoolSize = corePoolSize;
    this.maximumThreads = maximumThreads;
    this.keepAliveTime = keepAliveTime;
    this.timeUnit = timeUnit;
    this.threadFactoryConfig = threadFactoryConfig;
  }
  
  public int getCorePoolSize() {
    return corePoolSize;
  }
  
  public void setCorePoolSize(int corePoolSize) {
    this.corePoolSize = corePoolSize;
  }
  
  public int getMaximumThreads() {
    return maximumThreads;
  }
  
  public void setMaximumThreads(int maximumThreads) {
    this.maximumThreads = maximumThreads;
  }
  
  public int getKeepAliveTime() {
    return keepAliveTime;
  }
  
  public void setKeepAliveTime(int keepAliveTime) {
    this.keepAliveTime = keepAliveTime;
  }
  
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }
  
  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public ThreadFactoryConfig getThreadFactoryConfig() {
    return threadFactoryConfig;
  }

  public void setThreadFactoryConfig(ThreadFactoryConfig threadFactoryConfig) {
    this.threadFactoryConfig = threadFactoryConfig;
  }
  
}
