package org.ehcache.internal.executor;

import java.util.concurrent.ThreadPoolExecutor;

import org.ehcache.internal.executor.RequestContext.TaskPriority;



/**
 *  
 * {@link TaskPriority} based implementation of {@link ContextAnalyzer}. 
 * <p>
 * High task is processed by <b>exclusive</b> {@link ThreadPoolExecutor} while task with Normal priority is submitted to <b>shared</b> {@link ThreadPoolExecutor}
 * </P>
 * @author palmanojkumar
 *
 */
public class SimpleContextAnalyzer implements ContextAnalyzer {

  @Override
  public PoolType analyze(ExecutorServiceType type, RequestContext rContext, ExecutionContext eContext) {
    if (rContext.getTaskPriority() == TaskPriority.NORMAL) {
      return PoolType.SHARED;
    } else {
      return PoolType.EXCLUSIVE;
    }
  }

 
  
}
