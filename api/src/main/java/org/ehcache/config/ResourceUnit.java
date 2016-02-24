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
package org.ehcache.config;

/**
 * @author Ludovic Orban
 */
public interface ResourceUnit {

  /**
   * Compare {@code thisSize} of {@code this} unit to {@code thatSize} of
   * {@code thatUnit}.
   * <p>
   * Returns 1, 0, or -1 if the {@code thisSize} of {@code this} is greater than,
   * equal to, or less than {@code thatSize} of {@code thatUnit}
   * respectively.
   *
   * @param thisSize size in {@code this} unit
   * @param thatSize size in {@code thatUnit}
   * @param thatUnit other unit
   * @return as per the conventional compare contract
   * @throws IllegalArgumentException if the units are not comparable
   */
  int compareTo(long thisSize, long thatSize, ResourceUnit thatUnit) throws IllegalArgumentException;
}
