/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.config.builders;

import org.ehcache.expiry.ExpiryPolicy;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * ExpiryPolicyBuilderTest
 */
public class ExpiryPolicyBuilderTest {

  @Test
  public void testNoExpiration() {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.noExpiration();
    assertThat(expiry, sameInstance(ExpiryPolicy.NO_EXPIRY));
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(ExpiryPolicy.INFINITE));
    assertThat(expiry.getExpiryForAccess(this, () -> this), nullValue());
    assertThat(expiry.getExpiryForUpdate(this, () -> this, this), nullValue());
  }

  @Test
  public void testTTIExpiration() {
    java.time.Duration duration = java.time.Duration.ofSeconds(1L);
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, () -> this), equalTo(duration));
    assertThat(expiry.getExpiryForUpdate(this, () -> this, this), equalTo(duration));

    ExpiryPolicy<Object, Object> otherExpiry = ExpiryPolicyBuilder.timeToIdleExpiration(java.time.Duration.ofSeconds(1L));
    assertThat(otherExpiry, equalTo(expiry));
  }

  @Test
  public void testTTLExpiration() {
    java.time.Duration duration = java.time.Duration.ofSeconds(1L);
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToLiveExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, () -> this), nullValue());
    assertThat(expiry.getExpiryForUpdate(this, () -> this, this), equalTo(duration));

    ExpiryPolicy<Object, Object> otherExpiry = ExpiryPolicyBuilder.timeToLiveExpiration(java.time.Duration.ofSeconds(1L));
    assertThat(otherExpiry, equalTo(expiry));
  }

  @Test
  public void testExpiration() {
    Duration creation = Duration.ofSeconds(1L);
    Duration access = Duration.ofSeconds(2L);
    Duration update = Duration.ofSeconds(3L);
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.expiry().create(creation).access(access).update(update).build();
    assertThat(expiry.getExpiryForCreation(this, this), equalTo(creation));
    assertThat(expiry.getExpiryForAccess(this, () -> this), equalTo(access));
    assertThat(expiry.getExpiryForUpdate(this, () -> this,this), equalTo(update));
  }

  @Test
  public void testExpirationFunctions() {
    Duration creation = Duration.ofSeconds(1L);
    Duration access = Duration.ofSeconds(2L);
    Duration update = Duration.ofSeconds(3L);
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.expiry()
      .create((k, v) -> {
        assertThat(k, equalTo(10L));
        assertThat(v, equalTo(20L));
        return creation;
      })
      .access((k, v) -> {
        assertThat(k, equalTo(10L));
        assertThat(v.get(), equalTo(20L));
        return access;
      })
      .update((k, v1, v2) -> {
        assertThat(k, equalTo(10L));
        assertThat(v1.get(), equalTo(20L));
        assertThat(v2, equalTo(30L));
        return update;
      })
      .build();
    assertThat(expiry.getExpiryForCreation(10L, 20L), equalTo(creation));
    assertThat(expiry.getExpiryForAccess(10L, () -> 20L), equalTo(access));
    assertThat(expiry.getExpiryForUpdate(10L, () -> 20L,30L), equalTo(update));
  }
}
