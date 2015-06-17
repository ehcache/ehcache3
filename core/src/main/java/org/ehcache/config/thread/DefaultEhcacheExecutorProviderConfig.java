package org.ehcache.config.thread;

import org.ehcache.internal.executor.ContextAnalyzer;
import org.ehcache.internal.executor.RevisedEhcacheExecutorProvider;
import org.ehcache.internal.executor.ThreadFactoryProvider;
import org.ehcache.spi.service.ThreadPoolConfig;

/**
 * @author palmanojkumar
 *
 */
public final class DefaultEhcacheExecutorProviderConfig implements EhcacheExecutorProviderConfig {

  private ThreadPoolConfig sharedCachedThreadPoolConfig;
  private int sharedScheduledThreadPoolCoreSize;
  //private Class<? extends ContextAnalyzer> contextAnalyzerClass;
  private Class<? extends ThreadFactoryProvider> threadFactoryProvider;
  
  @Override
  public Class<RevisedEhcacheExecutorProvider> getServiceType() {

    return RevisedEhcacheExecutorProvider.class;
  }

  @Override
  public ThreadPoolConfig getSharedCachedThreadPoolConfig() {
    return sharedCachedThreadPoolConfig;
  }

  @Override
  public int getSharedScheduledThreadPoolCoreSize() {
    return sharedScheduledThreadPoolCoreSize;
  }


  @Override
  public Class<? extends ThreadFactoryProvider> getThreadFactoryProvider() {
    return threadFactoryProvider;
  }

  public void setSharedCachedThreadPoolConfig(ThreadPoolConfig sharedCachedThreadPoolConfig) {
    this.sharedCachedThreadPoolConfig = sharedCachedThreadPoolConfig;
  }

  public void setSharedScheduledThreadPoolCoreSize(int sharedScheduledThreadPoolCoreSize) {
    this.sharedScheduledThreadPoolCoreSize = sharedScheduledThreadPoolCoreSize;
  }

  public void setThreadFactoryProvider(Class<? extends ThreadFactoryProvider> threadFactoryProvider) {
    this.threadFactoryProvider = threadFactoryProvider;
  }
  
  /*public Class<? extends ContextAnalyzer> getContextAnalyzer() {
    return contextAnalyzerClass;
  }
  
  public void setContextAnalyzer(Class<? extends ContextAnalyzer> contextAnalyzer) {
    this.contextAnalyzerClass = contextAnalyzer;
  }*/
  
}
