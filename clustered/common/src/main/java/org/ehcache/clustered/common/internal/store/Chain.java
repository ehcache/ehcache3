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

package org.ehcache.clustered.common.internal.store;

import java.util.Iterator;

/**
 * Chain of element made of blobs
 * https://en.wikipedia.org/wiki/Chain.
 *
 * Technically, this is like a list of {@link Element}s.
 * This is server side data structure to store binary representation
 * of Cache data.
 *
 * This interface just allows iteration over elements at client side.
 *
 * This structure follows the insertion order, like an ordered list.
 * The {@link Iterator} returned by this class allows to traverse the chain
 * starting from first element.
 */
public interface Chain extends Iterable<Element> {

  /**
   * Returns true if Chain is empty else false.
   *
   * @return whether the Chain is empty
   */
  boolean isEmpty();

  /**
   * Returns the number of elements in this chain.
   *
   * @return the number of elements in this chain
   */
  int length();
}

