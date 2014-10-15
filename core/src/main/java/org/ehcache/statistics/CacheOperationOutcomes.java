package org.ehcache.statistics;

/**
 * 
 * @author Hung Huynh
 *
 */
public interface CacheOperationOutcomes {

  /**
   * Outcomes for cache Get operations.
   */
  enum GetOutcome {
    /** hit. */
    HIT,
    /** miss expired. */
    MISS_EXPIRED,
    /** miss not found. */
    MISS_NOT_FOUND
  };

  /**
   * The outcomes for Put Outcomes.
   */
  enum PutOutcome {
    /** added. */
    ADDED,
    /** updated. */
    UPDATED,

    /** ignored. */
    IGNORED
  };

  /**
   * The outcomes for remove operations.
   */
  enum RemoveOutcome {
    /** success. */
    SUCCESS
  };

  /**
   * The eviction outcomes.
   */
  enum EvictionOutcome {
    /** success. */
    SUCCESS
  };
}
