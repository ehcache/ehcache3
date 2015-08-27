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

package org.ehcache.util;

import java.io.Closeable;
import org.ehcache.spi.LifeCycled;

/**
 * Lifecycle utility methods.
 */
public class LifeCycleUtils {
  
  /**
   * Returns a {@code LifeCycled} instance that close the supplied closeable
   * upon lifecycle close.
   * 
   * @param closeable the closeable to close
   * @return a lifecycled instance that closes the closeable upon close
   */
  public static LifeCycled closerFor(final Closeable closeable) {
    return new LifeCycled() {

      @Override
      public void init() throws Exception {
        //nothing
      }

      @Override
      public void close() throws Exception {
        closeable.close();
      }
    };
  }
}
