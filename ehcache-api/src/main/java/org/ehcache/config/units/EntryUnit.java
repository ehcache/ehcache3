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
package org.ehcache.config.units;

import org.ehcache.config.ResourceUnit;

/**
 * A {@link ResourceUnit} that designates a count by entries.
 */
public enum EntryUnit implements ResourceUnit {
  /**
   * The only possible value of {@link EntryUnit}
   */
  ENTRIES {
    @Override
    public String toString() {
      return "entries";
    }

    @Override
    public int compareTo(long thisSize, long thatSize, ResourceUnit thatUnit) throws IllegalArgumentException {
      if (equals(thatUnit)) {
        return Long.signum(thisSize - thatSize);
      } else {
        throw new IllegalArgumentException();
      }
    }
  },
}
