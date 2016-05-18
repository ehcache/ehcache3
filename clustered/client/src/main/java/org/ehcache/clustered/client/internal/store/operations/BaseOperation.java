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
 * Base class that represents a {@link org.ehcache.Cache} operation
 *
 * @param <K> key type
 */
public abstract class BaseOperation<K> implements Operation<K> {

  protected final K key;

  public BaseOperation(final K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  @Override
  public String toString() {
    return getOpCode() + " key: " + key;
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof BaseOperation)) {
      return false;
    }

    BaseOperation<K> other = (BaseOperation)obj;
    if(this.getOpCode() != other.getOpCode()) {
      return false;
    }
    if(!this.getKey().equals(other.getKey())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return getOpCode().hashCode() + (key == null ? 0: key.hashCode());
  }
}
