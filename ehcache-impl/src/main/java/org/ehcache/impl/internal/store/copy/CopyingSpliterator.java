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

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

class CopyingSpliterator<T, U> implements Spliterator<U> {

  private final Spliterator<T> delegate;
  private final Function<T, U> copier;

  public CopyingSpliterator(Spliterator<T> delegate, Function<T, U> copier) {
    this.delegate = delegate;
    this.copier = copier;
  }

  @Override
  public boolean tryAdvance(Consumer<? super U> action) {
    return delegate.tryAdvance(t -> action.accept(copier.apply(t)));
  }

  @Override
  public Spliterator<U> trySplit() {
    Spliterator<T> split = delegate.trySplit();
    if (split == null) {
      return null;
    } else {
      return new CopyingSpliterator<>(split, copier);
    }
  }

  @Override
  public long estimateSize() {
    return delegate.estimateSize();
  }

  @Override
  public int characteristics() {
    return delegate.characteristics();
  }
}
