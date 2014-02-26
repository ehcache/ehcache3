/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.exceptions;

/**
 *
 * @author cdennis
 */
public class CacheAccessException extends Exception {

  public CacheAccessException(Throwable cause) {
    super(cause);
  }

  public CacheAccessException(String message, Throwable cause) {
    super(message, cause);
  }

  public CacheAccessException(String message) {
    super(message);
  }
  
}
