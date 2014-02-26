/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.exceptions;

/**
 *
 * @author cdennis
 */
public class PermanentPartialCacheAccessException extends CacheAccessException {

  public PermanentPartialCacheAccessException(String message) {
    super(message);
  }

  public PermanentPartialCacheAccessException(String message, Throwable cause) {
    super(message, cause);
  }

  public PermanentPartialCacheAccessException(Throwable cause) {
    super(cause);
  }

  
}
