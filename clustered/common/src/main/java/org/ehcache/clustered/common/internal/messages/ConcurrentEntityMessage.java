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

package org.ehcache.clustered.common.internal.messages;

import org.terracotta.entity.EntityMessage;

/**
 * {@link EntityMessage}s can implement this interface to specify which concurrency key
 * they belong to.
 */
public interface ConcurrentEntityMessage extends EntityMessage {

  /**
   * Get the {@link org.terracotta.entity.EntityMessage}'s concurrency key.
   *
   * @see org.terracotta.entity.ConcurrencyStrategy#concurrencyKey(EntityMessage)
   * @return the concurrency key
   */
  int concurrencyKey();

}
