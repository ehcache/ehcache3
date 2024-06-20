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

package org.ehcache.impl.internal.store.copy;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

class CopyingIterator<T, U> implements Iterator<U> {

  private final Iterator<T> delegate;
  private final Function<T, U> copier;

  CopyingIterator(java.util.Iterator<T> delegate, Function<T, U> copier) {
    this.delegate = delegate;
    this.copier = copier;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public U next() {
    return copier.apply(delegate.next());
  }

  @Override
  public void remove() {
    delegate.remove();
  }

  @Override
  public void forEachRemaining(Consumer<? super U> action) {
    delegate.forEachRemaining(t -> action.accept(copier.apply(t)));
  }
}

