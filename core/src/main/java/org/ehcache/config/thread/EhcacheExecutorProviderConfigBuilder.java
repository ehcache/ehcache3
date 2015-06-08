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

import org.ehcache.config.Builder;
import org.ehcache.spi.service.ThreadFactoryProvider;
import org.ehcache.spi.service.ThreadPoolConfig;

public class EhcacheExecutorProviderConfigBuilder implements Builder<EhcacheExecutorProviderConfig> {

  private ThreadPoolConfig sharedCachedThreadPoolConfig;
  private ThreadPoolConfig sharedScheduledThreadPoolConfig;
  private int maximumThreadsAllowed;
  private Class<? extends ThreadFactoryProvider> tfProvider;
  
  private EhcacheExecutorProviderConfigBuilder() {
  }
  
  private EhcacheExecutorProviderConfigBuilder(EhcacheExecutorProviderConfigBuilder other) {
    this.sharedCachedThreadPoolConfig = other.sharedCachedThreadPoolConfig;
    this.sharedScheduledThreadPoolConfig = other.sharedScheduledThreadPoolConfig;
    this.maximumThreadsAllowed = other.maximumThreadsAllowed;
    this.tfProvider = other.tfProvider;
  }
  
  public static EhcacheExecutorProviderConfigBuilder newEhcacheExecutorProviderconfigBuilder() {
    return new EhcacheExecutorProviderConfigBuilder();
  }
  
  public EhcacheExecutorProviderConfigBuilder sharedCachedThreadPoolConfig(ThreadPoolConfig poolConfig) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.sharedCachedThreadPoolConfig = poolConfig;
    return newBuilder;
  }
  
  public EhcacheExecutorProviderConfigBuilder sharedScheduledThreadPoolConfig(ThreadPoolConfig poolConfig) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.sharedScheduledThreadPoolConfig = poolConfig;
    return newBuilder;
  }
  
  public EhcacheExecutorProviderConfigBuilder maximumThreadsAllowed(int maximumThreadsAllowed) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.maximumThreadsAllowed = maximumThreadsAllowed;
    return newBuilder;
  }
  
  public EhcacheExecutorProviderConfigBuilder threadFactoryProvider(Class<? extends ThreadFactoryProvider> tfProvider) {
    EhcacheExecutorProviderConfigBuilder newBuilder = new EhcacheExecutorProviderConfigBuilder(this);
    newBuilder.tfProvider = tfProvider;
    return newBuilder;
  } 
  
  @Override
  public EhcacheExecutorProviderConfig build() {
    return null;
  }

}
