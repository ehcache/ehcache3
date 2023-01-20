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

package org.ehcache.impl.copy;

import org.ehcache.spi.copy.Copier;

/**
 * {@link ReadWriteCopier Copier} implementation that simply returns the value passed in, doing in fact no
 * copy at all.
 */
public final class IdentityCopier<T> extends ReadWriteCopier<T> {

  @SuppressWarnings("rawtypes")
  private static final Copier COPIER = new IdentityCopier<>();

  @SuppressWarnings("unchecked")
  public static <T> Copier<T> identityCopier() {
    return COPIER;
  }

  /**
   * This implementation returns the instance passed in as-is.
   */
  @Override
  public T copy(final T obj) {
    return obj;
  }
}
