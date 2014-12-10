package org.ehcache.jsr107;

import org.ehcache.expiry.Expiry;

/**
 * Eh107Expiry
 */
abstract class Eh107Expiry implements Expiry<Object, Object> {
  private final ThreadLocal<Object> shortCircuitAccess = new ThreadLocal<Object>();

  void enableShortCircuitAccessCalls() {
    shortCircuitAccess.set(this);
  }

  void disableShortCircuitAccessCalls() {
    shortCircuitAccess.remove();
  }

  boolean isShortCircuitAccessCalls() {
    return shortCircuitAccess.get() != null;
  }

}
