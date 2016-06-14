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

package org.ehcache.transactions.xa.internal.commands;

import org.ehcache.transactions.xa.internal.XAValueHolder;
import org.ehcache.transactions.xa.internal.XAStore;

/**
 * A representation of in-flight transaction's modification to the mappings of a {@link XAStore}.
 *
 * @author Ludovic Orban
 */
public interface Command<V> {

  /**
   * Get the value to rollback to.
   * @return the old value.
   */
  V getOldValue();

  /**
   * Get the value holder to commit.
   * @return the new value holder.
   */
  XAValueHolder<V> getNewValueHolder();

}
