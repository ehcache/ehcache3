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

package org.ehcache.internal.store.heap.holders;

import org.ehcache.spi.cache.AbstractValueHolder;

import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public abstract class OnHeapValueHolder<V> extends AbstractValueHolder<V> {

  public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  protected OnHeapValueHolder(long id, long creationTime) {
    super(id, creationTime);
  }

  protected OnHeapValueHolder(long id, long creationTime, long expirationTime) {
    super(id, creationTime, expirationTime);
  }

  @Override
  final protected TimeUnit nativeTimeUnit() {
    return TIME_UNIT;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && this.getClass().equals(obj.getClass())) {
      return super.equals(obj);
    }
    return false;
  }
}
