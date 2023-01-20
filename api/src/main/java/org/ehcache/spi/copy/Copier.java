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

package org.ehcache.spi.copy;

/**
 * Interface defining the contract used to make a copy of types.
 * <p>
 *   The copied object's class must be preserved in such a way that copying for read that object will return
 *   an object of the exact same class as before copy for write, i.e.: the following contract must always be true:
 *   <p>
 *   <code>object.getClass().equals( myCopier.copyForRead(myCopier.copyForWrite(object)).getClass() )</code>
 *   </p>
 * </p>
 * @param <T> the type of the instance to copy
 */
public interface Copier<T> {

  /**
   * Creates a copy of the instance passed in.
   *
   * @param obj the instance to copy
   * @return the copy of the {@code obj} instance
   */
  T copyForRead(T obj);

  /**
   * Creates a copy of the instance passed in.
   *
   * @param obj the instance to copy
   * @return the copy of the {@code obj} instance
   */
  T copyForWrite(T obj);
}
