package org.ehcache.internal.executor;



/**
 * @author palmanojkumar
 *
 */
public interface ContextAnalyzer {
  
  PoolType analyze(ExecutorServiceType type, RequestContext rContext, ExecutionContext eContext);
  
   enum PoolType {
    SHARED, EXCLUSIVE;
  }

}
