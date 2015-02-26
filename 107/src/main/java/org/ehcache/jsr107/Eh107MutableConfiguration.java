package org.ehcache.jsr107;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;

/**
 * Mutable configuration for Ehcache JSR107 Integration with method to figure out if the expiry policy has been explicitly set by user.
 */
public class Eh107MutableConfiguration<K, V> extends MutableConfiguration<K, V> {

  private boolean expiryPolicyOverridden = false;

  public Eh107MutableConfiguration() {
    super();
  }

  public Eh107MutableConfiguration(CompleteConfiguration<K, V> configuration) {
    super(configuration);
    if (configuration.getExpiryPolicyFactory() != null) {
      expiryPolicyOverridden = true;
    }
  }

  @Override
  public MutableConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> factory) {
    expiryPolicyOverridden = true;
    return super.setExpiryPolicyFactory(factory);
  }

  /**
   * Check if the user has explicitly overridden the expiry policy.
   * @return true if user has set the expiry policy
   */
  public boolean isExpiryPolicyOverridden() {
    return this.expiryPolicyOverridden;
  }
}
