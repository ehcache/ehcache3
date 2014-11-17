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
package org.ehcache.internal.store;

import org.ehcache.spi.cache.Store.ValueHolder;

class ByRefOnHeapValueHolder<V> extends BaseOnHeapValueHolder<V> {
  private final V value;

  protected ByRefOnHeapValueHolder(V value, long createTime) {
    super(createTime);
    if (value == null) {
      throw new NullPointerException("null value");
    }
    this.value = value;
  }
  
  @Override
  public final V value() {
    return value;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ValueHolder)) return false;
    return value.equals(((ValueHolder<?>)o).value());
  }
  
  @Override
  public int hashCode() {
    return value.hashCode();
  }

}
