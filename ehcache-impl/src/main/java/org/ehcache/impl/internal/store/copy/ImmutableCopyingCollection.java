/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

class ImmutableCopyingCollection<T> extends AbstractCollection<T> {

  private final Collection<T> delegate;
  private final UnaryOperator<T> copier;

  public ImmutableCopyingCollection(Collection<T> delegate, UnaryOperator<T> copier) {
    this.delegate = delegate;
    this.copier = copier;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return delegate.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new CopyingIterator<>(delegate.iterator(), copier);
  }

  @Override
  public void forEach(Consumer<? super T> action) {
    delegate.forEach(t -> action.accept(copier.apply(t)));
  }

  @Override
  public Object[] toArray() {
    return delegate.stream().map(copier).toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return delegate.stream().map(copier).collect(toList()).toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return delegate.containsAll(c);
  }

  @Override
  public Spliterator<T> spliterator() {
    return new CopyingSpliterator<>(delegate.spliterator(), copier);
  }

  @Override
  public Stream<T> stream() {
    return delegate.stream().map(copier);
  }

  @Override
  public Stream<T> parallelStream() {
    return delegate.parallelStream().map(copier);
  }
}
