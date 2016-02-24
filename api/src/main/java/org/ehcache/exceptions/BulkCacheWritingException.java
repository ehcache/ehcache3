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

package org.ehcache.exceptions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link CacheLoaderWriter} it uses threw an
 * exception while bulk writing / removing values for a given set of keys
 *
 * @author Anthony Dahanne
 */
public class BulkCacheWritingException extends CacheWritingException {

  private static final long serialVersionUID = -9019459887947633422L;

  private final Map<?, Exception> failures;
  private final Set<?> successes;

  /**
   * Constructs a new BulkCacheWritingException providing the key set that failed, including the exception loading these
   * threw, as well as all keys we managed to write a value for. This latter set of keys was
   * written successfully into the {@link org.ehcache.Cache}.
   *
   * @param failures the map of keys to failure encountered while loading the values
   * @param successes the set of keys successfully written / removed
   */
  public BulkCacheWritingException(final Map<?, Exception> failures, final Set<?> successes) {
    this.failures = Collections.unmodifiableMap(failures);
    this.successes = Collections.unmodifiableSet(successes);
  }

  /**
   * Accessor to all keys that failed loading during a bulk load operation, with the associated
   * {@link java.lang.Exception} encountered
   * @return a map of keys to exception encountered during bulk load
   */
  public Map<?, Exception> getFailures() {
    return failures;
  }

  /**
   * Accessor to all keys that were successfully loaded during a bulk load operation
   * @return a set of keys loaded and installed in the {@link org.ehcache.Cache}
   */
  public Set<?> getSuccesses() {
    return successes;
  }
}
