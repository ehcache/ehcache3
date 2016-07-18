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

package org.ehcache.core.spi;

/**
 * Adapter class for {@link LifeCycled} in case you do not need to implement all methods from the interface.
 */
public abstract class LifeCycledAdapter implements LifeCycled {

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() throws Exception {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws Exception {

  }
}
