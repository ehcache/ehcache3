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

package org.ehcache.internal.store.disk.ods;

/**
 * A region set based on an augmented AA-Tree.
 *
 * @author Chris Dennis
 */
class RegionSet extends AATreeSet<Region> {

  private final long size;

  /**
   * Construct the tree.
   */
  protected RegionSet(long size) {
    super();
    add(new Region(0, size - 1));
    this.size = size;
  }

  @Override
  public Region removeAndReturn(Object o) {
    Region r = super.removeAndReturn(o);
    if (r != null) {
      return new Region(r);
    } else {
      return null;
    }
  }

  @Override
  public Region find(Object o) {
    Region r = super.find(o);
    if (r != null) {
      return new Region(r);
    } else {
      return null;
    }
  }

  /**
   * Find a region of the the given size.
   */
  public Region find(long size) {
    Node<Region> currentNode = getRoot();
    Region currentRegion = currentNode.getPayload();

    if (currentRegion == null || size > currentRegion.contiguous()) {
      throw new IllegalArgumentException("Need to grow the region set");
    } else {
      while (true) {
        if (currentRegion.size() >= size) {
          return new Region(currentRegion.start(), currentRegion.start() + size - 1);
        } else {
          Region left = currentNode.getLeft().getPayload();
          Region right = currentNode.getRight().getPayload();
          if (left != null && left.contiguous() >= size) {
            currentNode = currentNode.getLeft();
            currentRegion = currentNode.getPayload();
          } else if (right != null && right.contiguous() >= size) {
            currentNode = currentNode.getRight();
            currentRegion = currentNode.getPayload();
          } else {
            throw new IllegalArgumentException("Couldn't find a " + size + " sized free area in " + currentRegion);
          }
        }
      }
    }
  }
}
