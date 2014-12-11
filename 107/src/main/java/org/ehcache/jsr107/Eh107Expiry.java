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
package org.ehcache.jsr107;

import org.ehcache.expiry.Expiry;

/**
 * Eh107Expiry
 */
abstract class Eh107Expiry<K, V> implements Expiry<K, V> {
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
