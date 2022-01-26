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

package org.ehcache.impl.internal.store.heap.holders;

import org.ehcache.core.spi.store.AbstractValueHolder;

import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public abstract class OnHeapValueHolder<V> extends AbstractValueHolder<V> {

  private final boolean evictionAdvice;
  private long size;

  protected OnHeapValueHolder(long id, long creationTime, boolean evictionAdvice) {
    super(id, creationTime);
    this.evictionAdvice = evictionAdvice;
  }

  protected OnHeapValueHolder(long id, long creationTime, long expirationTime, boolean evictionAdvice) {
    super(id, creationTime, expirationTime);
    this.evictionAdvice = evictionAdvice;
  }

  public boolean evictionAdvice() {
    return evictionAdvice;
  }

  public long size() {
    return this.size;
  }

  public void setSize(long size) {
    if (this.size != 0) {
      throw new UnsupportedOperationException("Cannot change the size if it is done already");
    }
    this.size = size;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && this.getClass().equals(obj.getClass())) {
      return super.equals(obj);
    }
    return false;
  }
}
