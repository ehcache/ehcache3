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

package org.ehcache.internal.store.disk;

/**
 * Filter on Elements or ElementSubstitutes.
 * <p/>
 * This is used in particular when selecting random samples from the store.
 *
 * @author Chris Dennis
 * @author Ludovic Orban
 */
public interface ElementSubstituteFilter {

  /**
   * Returns <code>true</code> if this object passes the filter.
   *
   * @param object object to test
   * @return <code>true</code> if passed
   */
  public boolean allows(Object object);
}
