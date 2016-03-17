package org.ehcache.impl.internal.store.offheap;

/**
 * Off-heap-backed maps counters.
 *
 * @author Ludovic Orban
 */
public interface OffHeapMapStatistics {

  long allocatedMemory();
  long occupiedMemory();
  long dataAllocatedMemory();
  long dataOccupiedMemory();
  long dataSize();
  long longSize();
  long tableCapacity();
  long usedSlotCount();
  long removedSlotCount();
  long reprobeLength();
  long vitalMemory();
  long dataVitalMemory();

}
