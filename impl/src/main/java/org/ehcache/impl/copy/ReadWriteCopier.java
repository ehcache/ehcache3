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
 * A helper {@link Copier} implementation that can be extended directly
 * if the copying operation is the same irrespective of the action
 * performed (read or write).
 */
public abstract class ReadWriteCopier<T> implements Copier<T> {

  /**
   * {@inheritDoc}
   */
  @Override
  public T copyForRead(final T obj) {
    return copy(obj);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T copyForWrite(final T obj) {
    return copy(obj);
  }

  /**
   * Template copy method to be implemented by sub-classes.
   * <P>
   *   It will be invoked when {@link #copyForRead(Object)} or {@link #copyForWrite(Object)} is invoked on
   *   {@link Copier}.
   * </P>
   *
   * @param obj the instance to copy
   *
   * @return a copied instance, depending on the implementation
   */
  public abstract T copy(T obj);
}
