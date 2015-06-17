package org.ehcache.internal.executor;

import org.ehcache.spi.service.ExecutorServiceType;

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
