package org.ehcache.jsr107;

import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;

/**
 * EhcacheExpiryWrapper
 */
class EhcacheExpiryWrapper extends Eh107Expiry {

  private final Expiry<Object, Object> wrappedExpiry;

  EhcacheExpiryWrapper(Expiry<Object, Object> wrappedExpiry) {
    this.wrappedExpiry = wrappedExpiry;
  }

  @Override
  public Duration getExpiryForCreation(Object key, Object value) {
    return wrappedExpiry.getExpiryForCreation(key, value);
  }

  @Override
  public Duration getExpiryForAccess(Object key, Object value) {
    return wrappedExpiry.getExpiryForAccess(key, value);
  }

  @Override
  public Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
    return wrappedExpiry.getExpiryForUpdate(key, oldValue, newValue);
  }
}
