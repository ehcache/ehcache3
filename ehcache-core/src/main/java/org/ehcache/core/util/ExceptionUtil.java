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
package org.ehcache.core.util;

public class ExceptionUtil {

  public static boolean containsCause(Throwable failure, Class<? extends Throwable> cause) {
    Throwable intermediate = failure;
    do {
      if (cause.isInstance(intermediate)) {
        return true;
      }
    } while ((intermediate = intermediate.getCause()) != null);

    return false;
  }


}
