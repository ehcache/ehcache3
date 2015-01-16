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
 * Class that represents the regions held within this set.
 *
 * @author Chris Dennis
 */
public class Region extends AATreeSet.AbstractTreeNode<Comparable> implements Comparable<Comparable> {
  private long start;
  private long end;

  private long contiguous;

  /**
   * Creates a region containing just the single given value
   *
   * @param value
   */
  public Region(long value) {
    this(value, value);
  }

  /**
   * Creates a region containing the given range of value (inclusive).
   */
  public Region(long start, long end) {
    super();
    this.start = start;
    this.end = end;
    updateContiguous();
  }

  /**
   * Create a shallow copy of a region.
   * <p/>
   * The new Region has NULL left and right children.
   */
  public Region(Region r) {
    this(r.start(), r.end());
  }

  /**
   * Return the size of the largest region linked from this node.
   */
  public long contiguous() {
    if (getLeft().getPayload() == null && getRight().getPayload() == null) {
      return size();
    } else {
      return contiguous;
    }
  }

  private void updateContiguous() {
    Region left = (Region) getLeft().getPayload();
    Region right = (Region) getRight().getPayload();
    long leftContiguous = left == null ? 0 : left.contiguous();
    long rightContiguous = right == null ? 0 : right.contiguous();
    contiguous = Math.max(size(), Math.max(leftContiguous, rightContiguous));
  }

  @Override
  public void setLeft(AATreeSet.Node<Comparable> l) {
    super.setLeft(l);
    updateContiguous();
  }

  @Override
  public void setRight(AATreeSet.Node<Comparable> r) {
    super.setRight(r);
    updateContiguous();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Range(" + this.start + "," + this.end + ")" + " contiguous:" + this.contiguous();
  }

  /**
   * Returns the size of this range (the number of values within its bounds).
   */
  public long size() {
    // since it is all inclusive
    return (isNull() ? 0 : this.end - this.start + 1);
  }

  /**
   * Return true if this region is null, i.e. represents no valid range.
   */
  protected boolean isNull() {
    return this.start > this.end;
  }

  /**
   * Remove the supplied region from this region.
   *
   * @param r region to remove
   * @return a possible extra region to be added, or null if none is required
   * @throws IllegalArgumentException if this region does not completely enclose the supplied region
   */
  protected Region remove(Region r) throws IllegalArgumentException {
    if (r.start < this.start || r.end > this.end) {
      throw new IllegalArgumentException("Ranges : Illegal value passed to remove : " + this + " remove called for : " + r);
    }
    if (this.start == r.start) {
      this.start = r.end + 1;
      updateContiguous();
      return null;
    } else if (this.end == r.end) {
      this.end = r.start - 1;
      updateContiguous();
      return null;
    } else {
      Region newRegion = new Region(r.end + 1, this.end);
      this.end = r.start - 1;
      updateContiguous();
      return newRegion;
    }
  }

  /**
   * Merge the supplied region into this region (if they are adjoining).
   *
   * @param r region to merge
   * @throws IllegalArgumentException if the regions are not adjoining
   */
  protected void merge(Region r) throws IllegalArgumentException {
    if (this.start == r.end + 1) {
      this.start = r.start;
    } else if (this.end == r.start - 1) {
      this.end = r.end;
    } else {
      throw new IllegalArgumentException("Ranges : Merge called on non contiguous values : [this]:" + this + " and " + r);
    }
    updateContiguous();
  }

  /**
   * {@inheritDoc}
   */
  public int compareTo(Comparable other) {
    if (other instanceof Region) {
      return compareTo((Region) other);
    } else if (other instanceof Long) {
      return compareTo((Long) other);
    } else {
      throw new AssertionError("Unusual Type " + other.getClass());
    }
  }

  private int compareTo(Region r) {
    if (this.start > r.start || this.end > r.end) {
      return 1;
    } else if (this.start < r.start || this.end < r.end) {
      return -1;
    } else {
      return 0;
    }
  }

  private int compareTo(Long l) {
    if (l.longValue() > end) {
      return -1;
    } else if (l.longValue() < start) {
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void swapPayload(AATreeSet.Node<Comparable> other) {
    if (other instanceof Region) {
      Region r = (Region) other;
      long temp = this.start;
      this.start = r.start;
      r.start = temp;
      temp = this.end;
      this.end = r.end;
      r.end = temp;
      updateContiguous();
    } else {
      throw new AssertionError();
    }
  }

  /**
   * {@inheritDoc}
   */
  public Region getPayload() {
    return this;
  }

  /**
   * Returns the start of this range (inclusive).
   */
  public long start() {
    return start;
  }

  /**
   * Returns the end of this range (inclusive).
   */
  public long end() {
    return end;
  }

}
