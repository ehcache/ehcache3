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

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class holds the results of the calls on each context.
 * <p/>
 * If a call was not possible to make on a context, because the context was not supported or the capability not found,
 * then the method {@link #hasResult()} will return false.
 * <p/>
 * You an call {@link #getResult(Class)} only if there has been a result, event if it is null.
 *
 * @author Mathieu Carbou
 */
public final class ContextualResult {

  private static final Object NO_RESULT = new Object();

  private final Object result;
  private final Map<String, String> context;

  private ContextualResult(Map<String, String> context, Object result) {
    this.result = result;
    this.context = Collections.unmodifiableMap(context);
  }

  public boolean hasResult() {
    return result != NO_RESULT;
  }

  public <T> T getResult(Class<T> type) throws NoSuchElementException {
    if (!hasResult()) {
      throw new NoSuchElementException();
    }
    return type.cast(result);
  }

  public Map<String, String> getContext() {
    return context;
  }

  public static ContextualResult result(Map<String, String> context, Object result) {
    return new ContextualResult(context, result);
  }

  public static ContextualResult noResult(Map<String, String> context) {
    return new ContextualResult(context, NO_RESULT);
  }

}
