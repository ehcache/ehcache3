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

import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;

/**
 * @author rism
 */
public class ResourcePoolMerger {

  public ResourcePools validateAndMerge(ResourcePools existing, ResourcePools toBeUpdated) {
    if(!existing.getResourceTypeSet().containsAll(toBeUpdated.getResourceTypeSet())) {
      throw new IllegalArgumentException("Pools to be updated cannot contain previously undefined resources pools");
    }
    if(toBeUpdated.getResourceTypeSet().contains(ResourceType.Core.OFFHEAP)) {
      throw new UnsupportedOperationException("Updating OFFHEAP resource is not supported");
    }
    for(ResourceType currentResourceType : toBeUpdated.getResourceTypeSet()) {
      if (toBeUpdated.getPoolForResource(currentResourceType).getSize() <= 0) {
        throw new IllegalArgumentException("Unacceptable size for resource pools provided");
      }
      if (toBeUpdated.getPoolForResource(currentResourceType).isPersistent() != 
          existing.getPoolForResource(currentResourceType).isPersistent()) {
        throw new IllegalArgumentException("Persistence configuration cannot be updated");
      }
      if(!toBeUpdated.getPoolForResource(currentResourceType).getUnit()
          .equals(existing.getPoolForResource(currentResourceType).getUnit())) {
        throw new UnsupportedOperationException("Updating ResourceUnit is not supported");
      }
    }

    ResourcePoolsBuilder mergedPoolsBuilder = ResourcePoolsBuilder.newResourcePoolsBuilder(existing);
    for(ResourceType currentResourceType : toBeUpdated.getResourceTypeSet()) {
      mergedPoolsBuilder = mergedPoolsBuilder.with(currentResourceType, toBeUpdated.getPoolForResource(currentResourceType).getSize(),
          toBeUpdated.getPoolForResource(currentResourceType).getUnit(), toBeUpdated.getPoolForResource(currentResourceType).isPersistent());
    }
    ResourcePools mergedResourcePools = mergedPoolsBuilder.build();

    if(mergedResourcePools.getPoolForResource(ResourceType.Core.DISK) != null
       && checkForTierSizingViolation(mergedResourcePools)) {
      throw new IllegalArgumentException("Updating resource pools leads authoritative tier being smaller than caching tier");
    }
    return mergedResourcePools;
  }

  /**
   * Check for a violation where Heap Store size is greater than Disk Store size.
   * Presently this check is only done for disk and heap store where both are defined with
   * {@link EntryUnit ENTRIES} as the unit of measure
   * @param pools {@link ResourcePools} resources pools that require a violation check
   * @return true if a violation is found
   */
  private static Boolean checkForTierSizingViolation(ResourcePools pools) {
    if (((pools.getPoolForResource(ResourceType.Core.HEAP).getUnit() == EntryUnit.ENTRIES)
         && (pools.getPoolForResource(ResourceType.Core.DISK).getUnit() == EntryUnit.ENTRIES))) {
      if ((pools.getPoolForResource(ResourceType.Core.HEAP).getSize()
           > pools.getPoolForResource(ResourceType.Core.DISK).getSize())) {
        return true;
      }
    }
    return false;
  }
}
