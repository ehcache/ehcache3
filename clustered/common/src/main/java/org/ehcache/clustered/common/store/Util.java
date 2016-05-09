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
package org.ehcache.clustered.common.store;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class Util {

  public static final <T> Iterator<T> reverseIterator(List<T> list) {
    final ListIterator<T> listIterator = list.listIterator(list.size());
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return listIterator.hasPrevious();
      }

      @Override
      public T next() {
        return listIterator.previous();
      }

      @Override
      public void remove() {
        listIterator.remove();
      }
    };
  }
}
