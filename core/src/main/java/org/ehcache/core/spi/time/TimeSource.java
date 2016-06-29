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

package org.ehcache.core.spi.time;

/**
 * A source of wall time.
 * <P>
 *   The main purpose of this interface is to allow tests to control time arbitrarily (as opposed to using system time
 *   and sleep()'ing to advance time.
 * </P>
 */
public interface TimeSource {

  /**
   * The current "time" in milliseconds
   */
  long getTimeMillis();

}
