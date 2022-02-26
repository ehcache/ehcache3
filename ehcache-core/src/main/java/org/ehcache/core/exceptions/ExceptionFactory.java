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

package org.ehcache.core.exceptions;

import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

/**
 * Factory to help creation of {@link CacheLoadingException} and {@link CacheWritingException}.
 */
public final class ExceptionFactory {

  private ExceptionFactory() {
    throw new UnsupportedOperationException("Thou shalt not instantiate me!");
  }

  /**
   * Creates a new {@code CacheWritingException} with the provided exception as cause.
   *
   * @param e the cause
   * @return a cache writing exception
   */
  public static CacheWritingException newCacheWritingException(Exception e) {
    return new CacheWritingException(e);
  }

  /**
   * Creates a new {@code CacheLoadingException} with the provided exception as cause.
   *
   * @param e the cause
   * @return a cache loading exception
   */
  public static CacheLoadingException newCacheLoadingException(Exception e) {
    return new CacheLoadingException(e);
  }

  /**
   * Creates a new {@code CacheWritingException} with the provided exception as cause and a suppressed one.
   *
   * @param e the cause
   * @param suppressed the suppressed exception to add to the new exception
   * @return a cache writing exception
   */
  public static CacheWritingException newCacheWritingException(Exception e, Exception suppressed) {
    CacheWritingException ne = new CacheWritingException(e);
    ne.addSuppressed(suppressed);
    return ne;
  }

  /**
   * Creates a new {@code CacheLoadingException} with the provided exception as cause and a suppressed one.
   *
   * @param e the cause
   * @param suppressed the suppressed exception to add to the new exception
   * @return a cache loading exception
   */
  public static CacheLoadingException newCacheLoadingException(Exception e, Exception suppressed) {
    CacheLoadingException ne = new CacheLoadingException(e);
    ne.addSuppressed(e);
    return ne;
  }
}
