package org.ehcache.expiry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class ExpirationsTest {

  @Test
  public void testNoExpiration() {
    Expiry<Object, Object> expiry = Expirations.noExpiration();
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(Duration.FOREVER));
    assertThat(expiry.getExpiryForAccess(this, this), nullValue());
  }

  @Test
  public void testTTIExpiration() {
    Duration duration = new Duration(1L, TimeUnit.SECONDS);
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, this), equalTo(duration));
  }

  @Test
  public void testTTLExpiration() {
    Duration duration = new Duration(1L, TimeUnit.SECONDS);
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, this), nullValue());
  }
}
