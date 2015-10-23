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
package org.ehcache.management;

import java.util.NoSuchElementException;

/**
 * This class holds the results of the calls on each context.
 * <p/>
 * If a call was not possible to make on a context, because the context was not supported or the capability not found,
 * then the method {@link #hasValue()} will return false.
 * <p/>
 * You an call {@link #getValue()} only if there has been a result, event if it is null.
 *
 * @author Mathieu Carbou
 */
public final class ContextualReturn<T> {

  private static final Object NO_RESULT = new Object();

  private final T value;
  private final Context context;

  private ContextualReturn(Context context, T value) {
    this.value = value;
    this.context = context;
  }

  public boolean hasValue() {
    return value != NO_RESULT;
  }

  public T getValue() throws NoSuchElementException {
    if (!hasValue()) {
      throw new NoSuchElementException();
    }
    return value;
  }

  public Context getContext() {
    return context;
  }

  public static <T> ContextualReturn<T> of(Context context, T result) {
    return new ContextualReturn<T>(context, result);
  }

  @SuppressWarnings("unchecked")
  public static <T> ContextualReturn<T> empty(Context context) {
    return new ContextualReturn<T>(Context.empty(), (T) NO_RESULT);
  }

}
