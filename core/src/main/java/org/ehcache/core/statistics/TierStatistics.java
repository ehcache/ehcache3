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

package org.ehcache.core.statistics;

import java.util.Map;

/**
 * All statistics relative to a tier
 */
public interface TierStatistics {

  Map<String, TypedValueStatistic> getKnownStatistics();

  /**
   * Reset the values for this tier. However, note that {@code mapping, maxMappings, allocatedMemory, occupiedMemory}
   * won't be reset since it doesn't make sense.
   * <p>
   * <b>Implementation note:</b> Calling clear doesn't really clear the data. It freezes the actual values and compensate
   * for them when returning a result.
   */
  void clear();

  long getHits();

  long getMisses();

  long getEvictions();

  long getMappings();

  long getMaxMappings();

  long getAllocatedByteSize();

  long getOccupiedByteSize();
}
