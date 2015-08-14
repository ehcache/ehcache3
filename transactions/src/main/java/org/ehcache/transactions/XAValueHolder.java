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
package org.ehcache.transactions;

import org.ehcache.spi.cache.AbstractValueHolder;

import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public class XAValueHolder<V> extends AbstractValueHolder<V> {
  private final V value;

  public XAValueHolder(long id, long creationTime, V value) {
    super(id, creationTime);
    this.value = value;
  }

  @Override
  protected TimeUnit nativeTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }

  @Override
  public V value() {
    return value;
  }
}
