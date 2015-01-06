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

/**
 * Exception thrown by a {@link org.ehcache.Cache} when the {@link org.ehcache.spi.loader.CacheLoader} it uses threw an
 * {@link java.lang.RuntimeException} while bulk loading values for a given set of keys
 *
 * @author Alex Snaps
 */
public class BulkCacheLoaderException extends CacheLoaderException {

  private static final long serialVersionUID = -5296309607929568779L;

  private final Map<?, Exception> failures;
  private final Map<?, ?> successes;

  /**
   * Constructs a new BulkCacheLoaderException providing the key set that failed, including the exception loading these
   * threw, as well as all keys we managed to load a value for, including the value loaded. This latter set of keys was
   * loaded successfully into the {@link org.ehcache.Cache}.
   *
   * @param failures the map of keys to failure encountered while loading the values
   * @param successes the map of keys successfully loaded and their associated values
   */
  public BulkCacheLoaderException(final Map<?, Exception> failures, final Map<?, ?> successes) {
    this.failures = Collections.unmodifiableMap(failures);
    this.successes = Collections.unmodifiableMap(successes);
  }

  /**
   * Constructs a new BulkCacheLoaderException providing a message, the key set that failed, including the exception
   * loading these threw, as well as all keys we managed to load a value for, including the value loaded. This latter
   * set of keys was loaded successfully into the {@link org.ehcache.Cache}.
   *
   * @param message the message
   * @param failures the map of keys to failure encountered while loading the values
   * @param successes the map of keys successfully loaded and their associated values
   * @see org.ehcache.exceptions.BulkCacheLoaderException#BulkCacheLoaderException(java.util.Map, java.util.Map)
   */
  public BulkCacheLoaderException(final String message, final Map<Object, Exception> failures, final Map<Object, Object> successes) {
    super(message);
    this.failures = Collections.unmodifiableMap(failures);
    this.successes = Collections.unmodifiableMap(successes);
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
   * Accessor to all keys that were successfully loaded during a bulk load operation, with the associated
   * loaded value
   * @return a map of keys to value loaded and installed in the {@link org.ehcache.Cache}
   */
  public Map<?, ?> getSuccesses() {
    return successes;
  }
}
