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

package org.ehcache.spi.loaderwriter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Thrown by a {@link org.ehcache.Cache} when its {@link CacheLoaderWriter}
 * fails while bulk mutating values.
 */
public class BulkCacheWritingException extends CacheWritingException {

  private static final long serialVersionUID = -9019459887947633422L;

  private final Map<?, Exception> failures;
  private final Set<?> successes;

  /**
   * Constructs a {@code BulkCacheWritingException} instance with the given map and set.
   * <p>
   * The given arguments are:
   * <ul>
   *   <li>a map from keys to exception thrown while writing,</li>
   *   <li>a set of keys for which writing succeeded</li>
   * </ul>
   *
   * @param failures the map of keys to failure encountered while loading
   * @param successes the map of keys successfully loaded and their associated value
   */
  public BulkCacheWritingException(Map<?, Exception> failures, Set<?> successes) {
    this.failures = Collections.unmodifiableMap(failures);
    this.successes = Collections.unmodifiableSet(successes);
  }

  /**
   * Returns the map of keys to exception.
   *
   * @return a map of keys to exception encountered while writing
   */
  public Map<?, Exception> getFailures() {
    return failures;
  }

  /**
   * Returns the set of keys that were successfully written.
   *
   * @return a set of keys successfully written
   */
  public Set<?> getSuccesses() {
    return successes;
  }

  @Override
  public String getMessage() {
    StringBuilder sb = new StringBuilder(13 + failures.size() * 20); // try to guess the final size
    sb.append("Failed keys:");
    failures.forEach((k, v) -> sb.append("\n ").append(k).append(" : ").append(v));
    return sb.toString();
  }

}
