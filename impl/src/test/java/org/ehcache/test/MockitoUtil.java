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

package org.ehcache.test;

import org.mockito.Mockito;

/**
 * Tiny little class allowing to remove the type constraint between the parameter and the return type. That way,
 * mocking a generic type won't be a systematic warning anymore.
 * <pre>{@code
 * List<String> list = MockitoUtil.mock(List.class); // no suppress warning
 * }
 * </pre>
 */
public final class MockitoUtil {

  private MockitoUtil() {
  }

  @SuppressWarnings("unchecked")
  public static <T> T mock(Class<?> clazz) {
    return Mockito.mock((Class<T>) clazz);
  }
}
