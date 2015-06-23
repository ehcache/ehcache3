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
package org.ehcache.internal.executor;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.ehcache.internal.executor.RequestContext.TaskPriority;
import org.ehcache.spi.ServiceProvider;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ThreadPoolConfig;

/**
 * 
 * 
 * @author palmanojkumar
 *
 */
public class DefaultEhcacheExecutorProvider implements RevisedEhcacheExecutorProvider {
  
  private ThreadPoolConfig cachedThreadPoolConfig;
  private int scheduledThreadPoolCoreSize;
  
  //private ContextAnalyzer contextAnalyzer;
  private ExecutorService sharedCachedThreadPoolExecutor;
  private ScheduledExecutorService sharedScheduledExecutor;
  private ExecutorService sharedSingleThreadExecutor;
  
  private long defaultTimeout = 60;
  
  private volatile boolean isServiceStarted;
  
  private Set<ExecutorService> managedExecutors = new CopyOnWriteArraySet<ExecutorService>();
  
  public DefaultEhcacheExecutorProvider() {
    
    //TODO: analyse thread/executor service usage for concrete numbers
    this(new ThreadPoolConfig(5, Integer.MAX_VALUE, 60, TimeUnit.MILLISECONDS, null), 4);
  }
  
  public DefaultEhcacheExecutorProvider(ThreadPoolConfig cachedThreadPoolConfig, int scheduledThreadPoolCoreSize) {
    this(cachedThreadPoolConfig, scheduledThreadPoolCoreSize, new SimpleContextAnalyzer());
  }
  
  public DefaultEhcacheExecutorProvider(ThreadPoolConfig cachedThreadPoolConfig, int scheduledThreadPoolCoreSize, ContextAnalyzer contextAnalyzer) {
    this.cachedThreadPoolConfig = cachedThreadPoolConfig;
    this.scheduledThreadPoolCoreSize = scheduledThreadPoolCoreSize;
    //this.contextAnalyzer = contextAnalyzer;
  }

  @Override
  public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    //TODO: it should be threadsafe..
    sharedCachedThreadPoolExecutor = new ThreadPoolExecutor(cachedThreadPoolConfig.getCorePoolSize(), cachedThreadPoolConfig.getMaximumThreads(), cachedThreadPoolConfig.getKeepAliveTime(), 
                                                            cachedThreadPoolConfig.getTimeUnit(), new SynchronousQueue<Runnable>(), new DefaultExecutionHandler());
    
    sharedSingleThreadExecutor = Executors.newSingleThreadExecutor();
    sharedScheduledExecutor = new ScheduledThreadPoolExecutor(scheduledThreadPoolCoreSize);
    
    managedExecutors.add(sharedCachedThreadPoolExecutor);
    managedExecutors.add(sharedScheduledExecutor);
    managedExecutors.add(sharedSingleThreadExecutor);
    
    isServiceStarted = true;
  }

  @Override
  public void stop() {
    isServiceStarted = false;

    for (ExecutorService eService : managedExecutors) {
      eService.shutdown();
      try {
        boolean terminated = eService.awaitTermination(defaultTimeout, TimeUnit.MILLISECONDS);
        if (!terminated) {
          eService.shutdownNow();
        }
      } catch (InterruptedException iex) {
        // log exception
      }
    }
  }

  private ExecutorService getSharedExecutorService(ExecutorServiceType eServiceType) {
    if (eServiceType == ExecutorServiceType.CACHED_THREAD_POOL) {
      return new ProxyExecutorService(sharedCachedThreadPoolExecutor, true);
    } else if (eServiceType == ExecutorServiceType.SINGLE_THREAD_EXECUTOR_SERVICE) {
      return new ProxyExecutorService(sharedSingleThreadExecutor, true);
    }
   return null;
  }
  
  private ExecutorService createExecutorService(PoolConfig config) {
    return null;
  }
  
  private void ensureServiceStarted() {
    if (!isServiceStarted) {
      throw new IllegalStateException("Service is not started.");
    }
  }
  
  @Override
  public ExecutorService getExecutorService(PoolConfig config, RequestContext context) {
    ensureServiceStarted();
    
    ExecutorService pExecutorService = null;
    //contextAnalyzer.analyze(config, context);
    
    if (context.getTaskPriority() == TaskPriority.NORMAL) {
      
      pExecutorService = getSharedExecutorService(config.getExecutorType());
      
    } else if (context.getTaskPriority() == TaskPriority.HIGH) {
      //create new executor service for exclusive usage..
      ExecutorService backingExecutorService = createExecutorService(config);
      managedExecutors.add(backingExecutorService);
      pExecutorService = new ProxyExecutorService(backingExecutorService);
    }
    
    //safe guard against null
    if (pExecutorService == null) {
      pExecutorService = new ProxyExecutorService(sharedCachedThreadPoolExecutor, true);
    }
    
    return pExecutorService;
  }

  @Override
  public ScheduledExecutorService getScheduledExecutorService(RequestContext context) {
    ensureServiceStarted();
    
    //TODO: 
    
    return null;
  }
  
  private class ProxyExecutorService implements ExecutorService {

    private ExecutorService backingExecutorService;
    private boolean isShared;

    public ProxyExecutorService(ExecutorService backingExecutorService) {
      this(backingExecutorService, false);
    }
    
    public ProxyExecutorService(ExecutorService backingExecutorService, boolean isShared) {
      this.backingExecutorService = backingExecutorService;
      this.isShared = isShared;
    }

    @Override
    public void execute(Runnable command) {
      backingExecutorService.execute(command);
    }

    @Override
    public void shutdown() {
      if (!isShared) {
        backingExecutorService.shutdown();
      }
      //ignore if backingThreadPoolExecutor is exclusive
    }

    @Override
    public List<Runnable> shutdownNow() {
      if (!isShared) {
        return backingExecutorService.shutdownNow();
      }
      
      //ignore shutdown command for shared thread pool
      return null;
    }

    @Override
    public boolean isShutdown() {
      return backingExecutorService.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return backingExecutorService.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return backingExecutorService.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return backingExecutorService.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return backingExecutorService.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
      return backingExecutorService.submit(task);
    }

    
    //decide whether we need to support these bulk methods....
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return backingExecutorService.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      return backingExecutorService.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      return backingExecutorService.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return backingExecutorService.invokeAny(tasks, timeout, unit);
    }

  }
  
  
  private class ProxyScheduledExecutorService extends ProxyExecutorService implements ScheduledExecutorService {

    private ScheduledExecutorService backingScheduledExecutorService;
   
    public ProxyScheduledExecutorService(ScheduledExecutorService backingScheduledExecutorService) {
      this(backingScheduledExecutorService, false);
    }
    
    public ProxyScheduledExecutorService(ScheduledExecutorService backingScheduledExecutorService, boolean isShared) {
      super(backingScheduledExecutorService, isShared);
      this.backingScheduledExecutorService = backingScheduledExecutorService;
    }
    
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return backingScheduledExecutorService.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return backingScheduledExecutorService.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return backingScheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return backingScheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
    
  }
 
  private class DefaultExecutionHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
      // If runnable is {@link EhcacheManagedTask} then notify them about this
      // failure.

      if (runnable instanceof EhcacheManagedTask) {
        ((EhcacheManagedTask) runnable).getTaskListener().taskRejected();
      }

    }

  }

}