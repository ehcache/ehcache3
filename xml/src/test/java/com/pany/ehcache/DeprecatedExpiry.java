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

package com.pany.ehcache;

import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
@SuppressWarnings("deprecation")
public class DeprecatedExpiry implements org.ehcache.expiry.Expiry<Object, Object> {
  @Override
  public org.ehcache.expiry.Duration getExpiryForCreation(Object key, Object value) {
    return org.ehcache.expiry.Duration.of(42, TimeUnit.SECONDS);
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForAccess(Object key, org.ehcache.ValueSupplier<? extends Object> value) {
    return org.ehcache.expiry.Duration.of(42, TimeUnit.SECONDS);
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForUpdate(Object key, org.ehcache.ValueSupplier<? extends Object> oldValue, Object newValue) {
    return org.ehcache.expiry.Duration.of(42, TimeUnit.SECONDS);
  }
}
