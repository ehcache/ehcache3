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
package org.ehcache.impl.internal.loaderwriter.writebehind.operations;

/**
 * Interface to implement operations that are performed with write-behind and based on a key
 *
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public interface KeyBasedOperation<K> {

  /**
   * Retrieves the element key for this operation.
   *
   * @return the key of the element that is concerned by the operation.
   */
  K getKey();

  /**
   * Retrieves the moment when the operation was created.
   *
   * @return the creation time in milliseconds
   */
  long getCreationTime();

}
