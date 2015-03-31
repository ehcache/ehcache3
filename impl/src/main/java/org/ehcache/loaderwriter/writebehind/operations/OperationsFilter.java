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
package org.ehcache.loaderwriter.writebehind.operations;

import java.util.List;

/**
 * An interface for implementing a filter for operations before they are processed. By filtering the outstanding
 * operations it's for example possible to remove scheduled work before it's actually executed.
 * 
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public interface OperationsFilter<T> {

  /**
   * Filter the operations of a write behind queue.
   *
   * @param operations the operations to filter
   */
  void filter(List<T> operations);
  
}
