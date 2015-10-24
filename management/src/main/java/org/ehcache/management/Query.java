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

import java.util.List;
import java.util.Map;

/**
 * @author Mathieu Carbou
 */
public interface Query<T> {

  /**
   * @return The capability name used for this query
   */
  String getCapabilityName();

  /**
   * @return The list of context targetted by this query
   */
  List<Map<String, String>> getContexts();

  /**
   * @return The list of results of this query against all contexts, in the same order and same position of the {@link #getContexts()} list
   */
  List<T> execute();

}
