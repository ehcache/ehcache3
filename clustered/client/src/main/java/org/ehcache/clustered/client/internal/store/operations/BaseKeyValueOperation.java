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

package org.ehcache.clustered.client.internal.store.operations;

/**
 * Base class that represents {@link org.ehcache.Cache} operations
 * that involves a key and a value.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class BaseKeyValueOperation<K, V> extends BaseOperation<K> implements KeyValueOperation<K, V> {

  protected final V value;

  public BaseKeyValueOperation(final K key, final V value) {
    super(key);
    this.value = value;
  }

  public V getValue() {
    return this.value;
  }

  @Override
  public String toString() {
    return super.toString() + ", value: " + value;
  }

  @Override
  public boolean equals(final Object obj) {
    if(!super.equals(obj)) {
      return false;
    }
    if(!(obj instanceof BaseKeyValueOperation)) {
      return false;
    }

    BaseKeyValueOperation<K, V> other = (BaseKeyValueOperation)obj;
    if(!this.getValue().equals(other.getValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + (value == null? 0: value.hashCode());
  }
}
