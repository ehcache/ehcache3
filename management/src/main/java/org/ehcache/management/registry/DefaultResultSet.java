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
package org.ehcache.management.registry;

import org.ehcache.management.Context;
import org.ehcache.management.ResultSet;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Mathieu Carbou
 */
final class DefaultResultSet<T> implements ResultSet<T> {

  private final Map<Context, T> results;

  DefaultResultSet(Map<Context, T> results) {
    this.results = results;
  }

  @Override
  public T getResult(Context context) {
    return results.get(context);
  }

  @Override
  public T getSingleResult() throws NoSuchElementException {
    if (results.size() != 1) {
      throw new NoSuchElementException();
    }
    return results.values().iterator().next();
  }

  @Override
  public boolean isEmpty() {
    return results.isEmpty();
  }

  @Override
  public int size() {
    return results.size();
  }

  @Override
  public Iterator<T> iterator() {
    return results.values().iterator();
  }

}
