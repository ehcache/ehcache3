package org.ehcache.internal.executor;

import org.ehcache.spi.service.ExecutorServiceType;

/**
 * Analyze different stats provided by {@link ExecutionContext} alongwith {@link RequestContext} to decide pool type 
 * 
 * @author palmanojkumar
 *
 */
public class StatBasedContextAnalyzer implements ContextAnalyzer {

  @Override
  public PoolType analyze(ExecutorServiceType type, RequestContext rContext, ExecutionContext eContext) {

    return null;
  }

}
