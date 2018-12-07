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

package org.ehcache.docs;

import org.ehcache.expiry.ExpiryPolicy;
import org.junit.Test;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Samples showing performance strategies.
 */
public class Performance {

  @Test
  public void expiryAllocation() {
    // tag::expiryAllocation[]
    new ExpiryPolicy<Object, Object>() {
      @Override
      public Duration getExpiryForCreation(Object key, Object value) {
        return null;
      }

      @Override
      public Duration getExpiryForAccess(Object key, Supplier<?> value) {
        return Duration.ofSeconds(10); // <1>
      }

      @Override
      public Duration getExpiryForUpdate(Object key, Supplier<?> oldValue, Object newValue) {
        return null;
      }
    };
    // end::expiryAllocation[]
  }
}
