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
package org.ehcache.expiry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.TimeUnit;

import org.ehcache.ValueSupplier;
import org.junit.Test;

public class ExpirationsTest {

  @Test
  public void testNoExpiration() {
    Expiry<Object, Object> expiry = Expirations.noExpiration();
    assertThat(expiry.getExpiryForCreation(this, holderOf(this)), equalTo(Duration.INFINITE));
    assertThat(expiry.getExpiryForAccess(this, holderOf(this)), nullValue());
    assertThat(expiry.getExpiryForUpdate(this, holderOf(this), holderOf(this)), nullValue());
  }

  @Test
  public void testTTIExpiration() {
    Duration duration = new Duration(1L, TimeUnit.SECONDS);
    Expiry<Object, Object> expiry = Expirations.timeToIdleExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, holderOf(this)), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, holderOf(this)), equalTo(duration));
    assertThat(expiry.getExpiryForUpdate(this, holderOf(this), holderOf(this)), equalTo(duration));
  }

  @Test
  public void testTTLExpiration() {
    Duration duration = new Duration(1L, TimeUnit.SECONDS);
    Expiry<Object, Object> expiry = Expirations.timeToLiveExpiration(duration);
    assertThat(expiry.getExpiryForCreation(this, holderOf(this)), equalTo(duration));
    assertThat(expiry.getExpiryForAccess(this, holderOf(this)), nullValue());
    assertThat(expiry.getExpiryForUpdate(this, holderOf(this), holderOf(this)), equalTo(duration));
  }

  private ValueSupplier<Object> holderOf(final Object obj) {
    return new ValueSupplier<Object>() {
      @Override
      public Object value() {
        return obj;
      }
    };
  }
}
