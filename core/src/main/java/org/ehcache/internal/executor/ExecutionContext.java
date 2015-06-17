package org.ehcache.internal.executor;

/**
 * 
 * @author palmanojkumar
 *
 */
public interface ExecutionContext {

  String getCacheName();
  
  void getSharedCachedThreadPoolStatus();
  void getSharedSingleThreadPoolStatus();
  void getSharedScheduledThreadPoolStatus();
  
  /**
   * Returns total task submitted under specified {@link RequestContext}
   * @return
   */
  int totalTaskSubmitted();
  
  
  public class PoolStatus {
    private int queuedTask;
    private int processingThreadCount;
    private int maximumThreadsAllowed;
    
    public PoolStatus(int queuedTask, int processingThreadCount, int maximumThreadsAllowed) {
      this.queuedTask = queuedTask;
      this.processingThreadCount = processingThreadCount;
      this.maximumThreadsAllowed = maximumThreadsAllowed;
    }
    
    public int getQueuedTask() {
      return queuedTask;
    }
 
    public int getProcessingThreadCount() {
      return processingThreadCount;
    }

    public int getMaximumThreadsAllowed() {
      return maximumThreadsAllowed;
    }
    
  }
}
