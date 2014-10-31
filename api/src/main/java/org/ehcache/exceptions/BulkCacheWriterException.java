package org.ehcache.exceptions;

import java.util.Collections;
import java.util.Map;

/**
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link org.ehcache.spi.writer.CacheWriter} it uses threw an
 * {@link java.lang.RuntimeException} while bulk writing values for a given set of keys
 *
 * @author Anthony Dahanne
 */
public class BulkCacheWriterException extends CacheWriterException {
  private final Map<?, Exception> failures;
  private final Map<?, ?> successes;

  /**
   * Constructs a new BulkCacheWriterException providing the key set that failed, including the exception loading these
   * threw, as well as all keys we managed to write a value for, including the value written. This latter set of keys was
   * written successfully into the {@link org.ehcache.Cache}.
   *
   * @param failures the map of keys to failure encountered while loading the values
   * @param successes the map of keys successfully written and their associated values
   */
  public BulkCacheWriterException(final Map<?, Exception> failures, final Map<?, ?> successes) {
    this.failures = Collections.unmodifiableMap(failures);
    this.successes = Collections.unmodifiableMap(successes);
  }

  /**
   * Accessor to all keys that failed loading during a bulk load operation, with the associated
   * {@link java.lang.Exception} encountered
   * @return a map of keys to exception encountered during bulk load
   */
  public Map<?, Exception> getFailures() {
    return failures;
  }

  /**
   * Accessor to all keys that were successfully loaded during a bulk load operation, with the associated
   * loaded value
   * @return a map of keys to value loaded and installed in the {@link org.ehcache.Cache}
   */
  public Map<?, ?> getSuccesses() {
    return successes;
  }
}
