package org.ehcache.internal.store.disk.utils;

import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * Created by lorban on 08/01/15.
 */
public class CacheException extends RuntimeException {
  public CacheException(String s, Throwable t) {
    super(s, t);
  }

  public CacheException(Throwable t) {
    super(t);
  }

  public CacheException(String s) {
    super(s);
  }
}
