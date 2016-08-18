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

import org.ehcache.ValueSupplier;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;

import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
public class MyExpiry implements Expiry<Object, Object> {
  @Override
  public Duration getExpiryForCreation(final Object key, final Object value) {
    return new Duration(42, TimeUnit.SECONDS);
  }

  @Override
  public Duration getExpiryForAccess(final Object key, final ValueSupplier<? extends Object> value) {
    return new Duration(42, TimeUnit.SECONDS);
  }

  @Override
  public Duration getExpiryForUpdate(Object key, ValueSupplier<? extends Object> oldValue, Object newValue) {
    return new Duration(42, TimeUnit.SECONDS);
  }
}
