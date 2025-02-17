/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

/**
 * @author teck
 */
final class Unwrap {

  static <T> T unwrap(Class<T> clazz, Object... obj) {
    requireNonNull(clazz);
    return stream(obj).filter(clazz::isInstance).map(clazz::cast).findFirst()
      .orElseThrow(() -> new IllegalArgumentException("Cannot unwrap to " + clazz));
  }

  private Unwrap() {
    //
  }
}
