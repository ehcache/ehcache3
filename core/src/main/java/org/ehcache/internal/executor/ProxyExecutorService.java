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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author palmanojkumar
 *
 */
public class ProxyExecutorService implements ExecutorService {

  private static final String OPERATION_NOT_SUPPORTED = "Lifecycle operation not supported";
  
  private ExecutorService backingExecutorService;
  private InternalExecutionContext iContext;
  private PoolConfig config;
  
  public ProxyExecutorService(PoolConfig config, ExecutionContext eContext) {
    this.config = config;
    this.iContext = new InternalExecutionContext(eContext);
  }
  
  @Override
  public void execute(Runnable command) {
    //update internal execution context stats i.e. 1. total number of request processed  2. frequency of task submitted 
    //examine updated execution context and pool config to determine type of executor service to use
    //update backingExecutorService and submit task to process it.
    //if task rate is high then relase existing backingExecutorService, create and schedule in exclusive service.
    
    backingExecutorService.execute(command);
    
    //update internal execution context stats. 3. Average time
  }

  @Override
  public void shutdown() {
    //if shared then ignore shutdown request.
    //if exclusive executor service then accept and delegate.
    
  }

  @Override
  public List<Runnable> shutdownNow() {
    //if shared then ignore shutdown request.
    //if exclusive executor service then accept and delegate.
    return null;
  }

  @Override
  public boolean isShutdown() {
    //if shared then ignore shutdown request.
    //if exclusive executor service then accept and delegate.
    return false;
  }

  @Override
  public boolean isTerminated() {
    //if shared then ignore shutdown request.
    //if exclusive executor service then accept and delegate.
    throw new IllegalStateException(OPERATION_NOT_SUPPORTED);
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    //if shared then ignore shutdown request.
    //if exclusive executor service then accept and delegate.
    throw new IllegalStateException(OPERATION_NOT_SUPPORTED);
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

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    //we should not support bulk submissions
    throw new IllegalStateException();
    //return null;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
    //we should not support bulk submissions
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    //we should not support bulk submissions
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    //we should not support bulk submissions
    return null;
  }
  
  
  private class InternalExecutionContext {
    
    private int totalTaskProcessed;
    private long taskFrequency;
    private long averageTime;
    
    
    public InternalExecutionContext(ExecutionContext eContext) {
      
    }
    
    public void updateTotalTaskProcessed() {
      
    }
    
    public void updateTaskFrequency() {
      
    }
    
    public void updateAverageTime() {
      
    }
    
    
    
  }

}
