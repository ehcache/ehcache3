/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class CopyingIterable<T, U> implements Iterable<U> {

  private final Iterable<T> delegate;
  private final Function<T, U> copier;

  CopyingIterable(Iterable<T> delegate, Function<T, U> mapping) {
    this.delegate = delegate;
    this.copier = mapping;
  }

  @Override
  public Iterator<U> iterator() {
    return new CopyingIterator<>(delegate.iterator(), copier);
  }

  @Override
  public void forEach(Consumer<? super U> action) {
    delegate.forEach(t -> action.accept(copier.apply(t)));
  }

  @Override
  public Spliterator<U> spliterator() {
    return new CopyingSpliterator<>(delegate.spliterator(), copier);
  }
}
