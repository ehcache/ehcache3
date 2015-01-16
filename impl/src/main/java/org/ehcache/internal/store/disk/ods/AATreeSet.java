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

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.Stack;

/**
 * A AA-Tree based SortedSet implementation
 *
 * @param <T> type of values stored
 * @author Chris Dennis
 */
public class AATreeSet<T extends Comparable> extends AbstractSet<T> implements SortedSet<T> {

  private static final Node<?> TERMINAL = new TerminalNode();

  private Node<T> root = terminal();

  private int size;
  private boolean mutated;

  private Node<T> item = terminal();
  private Node<T> heir = terminal();
  private T removed;

  @Override
  public boolean add(T o) {
    try {
      root = insert(root, o);
      if (mutated) {
        size++;
      }
      return mutated;
    } finally {
      mutated = false;
    }
  }

  @Override
  public boolean remove(Object o) {
    try {
      root = remove(root, (T) o);
      if (mutated) {
        size--;
      }
      return mutated;
    } finally {
      heir = terminal();
      item = terminal();
      mutated = false;
      removed = null;
    }
  }

  /**
   * Remove the node matching this object and return it.
   */
  public T removeAndReturn(Object o) {
    try {
      root = remove(root, (T) o);
      if (mutated) {
        size--;
      }
      return removed;
    } finally {
      heir = terminal();
      item = terminal();
      mutated = false;
      removed = null;
    }
  }

  @Override
  public void clear() {
    root = terminal();
    size = 0;
  }

  @Override
  public Iterator<T> iterator() {
    return new TreeIterator();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return root == terminal();
  }

  /**
   * {@inheritDoc}
   */
  public Comparator<? super T> comparator() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public SortedSet<T> subSet(T fromElement, T toElement) {
    return new SubSet(fromElement, toElement);
  }

  /**
   * {@inheritDoc}
   */
  public SortedSet<T> headSet(T toElement) {
    return new SubSet(null, toElement);
  }

  /**
   * {@inheritDoc}
   */
  public SortedSet<T> tailSet(T fromElement) {
    return new SubSet(fromElement, null);
  }

  /**
   * {@inheritDoc}
   */
  public T first() {
    Node<T> leftMost = root;
    while (leftMost.getLeft() != terminal()) {
      leftMost = leftMost.getLeft();
    }
    return leftMost.getPayload();
  }

  /**
   * {@inheritDoc}
   */
  public T last() {
    Node<T> rightMost = root;
    while (rightMost.getRight() != terminal()) {
      rightMost = rightMost.getRight();
    }
    return rightMost.getPayload();
  }

  /**
   * Find the node within this tree equal to the probe node.
   */
  public T find(Object probe) {
    return find(root, (T) probe).getPayload();
  }

  private Node<T> terminal() {
    return (Node<T>) TERMINAL;
  }

  /**
   * Returns the root node of this tree.
   */
  protected final Node<T> getRoot() {
    return root;
  }

  private Node<T> find(Node<T> top, T probe) {
    if (top == terminal()) {
      return top;
    } else {
      int direction = top.compareTo(probe);
      if (direction > 0) {
        return find(top.getLeft(), probe);
      } else if (direction < 0) {
        return find(top.getRight(), probe);
      } else {
        return top;
      }
    }
  }

  private Node<T> insert(Node<T> top, T data) {
    if (top == terminal()) {
      mutated = true;
      return createNode(data);
    } else {
      int direction = top.compareTo(data);
      if (direction > 0) {
        top.setLeft(insert(top.getLeft(), data));
      } else if (direction < 0) {
        top.setRight(insert(top.getRight(), data));
      } else {
        return top;
      }
      top = skew(top);
      top = split(top);
      return top;
    }
  }

  private Node<T> createNode(T data) {
    if (data instanceof Node<?>) {
      return (Node<T>) data;
    } else {
      return new TreeNode<T>(data);
    }
  }

  private Node<T> remove(Node<T> top, T data) {
    if (top != terminal()) {
      int direction = top.compareTo(data);

      heir = top;
      if (direction > 0) {
        top.setLeft(remove(top.getLeft(), data));
      } else {
        item = top;
        top.setRight(remove(top.getRight(), data));
      }

      if (top == heir) {
        if (item != terminal() && item.compareTo(data) == 0) {
          mutated = true;
          item.swapPayload(top);
          removed = top.getPayload();
          top = top.getRight();
        }
      } else {
        if (top.getLeft().getLevel() < top.getLevel() - 1 || top.getRight().getLevel() < top.getLevel() - 1) {
          if (top.getRight().getLevel() > top.decrementLevel()) {
            top.getRight().setLevel(top.getLevel());
          }

          top = skew(top);
          top.setRight(skew(top.getRight()));
          top.getRight().setRight(skew(top.getRight().getRight()));
          top = split(top);
          top.setRight(split(top.getRight()));
        }
      }
    }
    return top;
  }

  private static <T> Node<T> skew(Node<T> top) {
    if (top.getLeft().getLevel() == top.getLevel() && top.getLevel() != 0) {
      Node<T> save = top.getLeft();
      top.setLeft(save.getRight());
      save.setRight(top);
      top = save;
    }

    return top;
  }

  private static <T> Node<T> split(Node<T> top) {
    if (top.getRight().getRight().getLevel() == top.getLevel() && top.getLevel() != 0) {
      Node<T> save = top.getRight();
      top.setRight(save.getLeft());
      save.setLeft(top);
      top = save;
      top.incrementLevel();
    }

    return top;
  }

  /**
   * Interface implemented by nodes within this tree.
   */
  public static interface Node<E> {

    /**
     * Compare this node to the supplied 'data' object.
     */
    public int compareTo(E data);

    /**
     * Set this node's left child.
     */
    public void setLeft(Node<E> node);

    /**
     * Set this node's right child.
     */
    public void setRight(Node<E> node);

    /**
     * Get this node's left child.
     */
    public Node<E> getLeft();

    /**
     * Get this node's right child.
     */
    public Node<E> getRight();

    /**
     * Get this node's level.
     */
    public int getLevel();

    /**
     * Set this node's level.
     */
    public void setLevel(int value);

    /**
     * Decrement and then return this node's new level.
     */
    public int decrementLevel();

    /**
     * Increment and then return this node's new level.
     */
    public int incrementLevel();

    /**
     * Swap the payload objects between this node and the supplied node.
     */
    public void swapPayload(Node<E> with);

    /**
     * Return the 'value' object held within this node.
     */
    public E getPayload();
  }

  /**
   * Abstract node implementation that can be extended with a custom payload.
   */
  public abstract static class AbstractTreeNode<E> implements Node<E> {

    private Node<E> left;
    private Node<E> right;
    private int level;

    /**
     * Constructs an initial (leaf) node.
     */
    public AbstractTreeNode() {
      this(1);
    }

    private AbstractTreeNode(int level) {
      this.left = (Node<E>) TERMINAL;
      this.right = (Node<E>) TERMINAL;
      this.level = level;
    }

    /**
     * {@inheritDoc}
     */
    public void setLeft(Node<E> node) {
      left = node;
    }

    /**
     * {@inheritDoc}
     */
    public void setRight(Node<E> node) {
      right = node;
    }

    /**
     * {@inheritDoc}
     */
    public Node<E> getLeft() {
      return left;
    }

    /**
     * {@inheritDoc}
     */
    public Node<E> getRight() {
      return right;
    }

    /**
     * {@inheritDoc}
     */
    public int getLevel() {
      return level;
    }

    /**
     * {@inheritDoc}
     */
    public void setLevel(int value) {
      level = value;
    }

    /**
     * {@inheritDoc}
     */
    public int decrementLevel() {
      return --level;
    }

    /**
     * {@inheritDoc}
     */
    public int incrementLevel() {
      return ++level;
    }
  }

  /**
   * Default Comparable wrapping node implementation.
   */
  private static final class TreeNode<E extends Comparable> extends AbstractTreeNode<E> {

    private E payload;

    public TreeNode(E payload) {
      super();
      this.payload = payload;
    }

    public int compareTo(E data) {
      return payload.compareTo(data);
    }

    public void swapPayload(Node<E> node) {
      if (node instanceof TreeNode<?>) {
        TreeNode<E> treeNode = (TreeNode<E>) node;
        E temp = treeNode.payload;
        treeNode.payload = this.payload;
        this.payload = temp;
      } else {
        throw new IllegalArgumentException();
      }
    }

    public E getPayload() {
      return payload;
    }
  }

  /**
   * Node implementation that is used to mark the terminal (leaf) nodes of the tree.
   */
  private static final class TerminalNode<E> extends AbstractTreeNode<E> {

    TerminalNode() {
      super(0);
      super.setLeft(this);
      super.setRight(this);
    }

    public int compareTo(E data) {
      return 0;
    }

    @Override
    public void setLeft(Node<E> right) {
      if (right != TERMINAL) {
        throw new AssertionError();
      }
    }

    @Override
    public void setRight(Node<E> left) {
      if (left != TERMINAL) {
        throw new AssertionError();
      }
    }

    @Override
    public void setLevel(int value) {
      throw new AssertionError();
    }

    @Override
    public int decrementLevel() {
      throw new AssertionError();
    }

    @Override
    public int incrementLevel() {
      throw new AssertionError();
    }

    public void swapPayload(Node<E> payload) {
      throw new AssertionError();
    }

    public E getPayload() {
      return null;
    }
  }

  /**
   * SortedSet representation of a range of the tree.
   */
  private class SubSet extends AbstractSet<T> implements SortedSet<T> {

    private final T start;
    private final T end;

    SubSet(T start, T end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public boolean add(T o) {
      if (inRange(o)) {
        return AATreeSet.this.add(o);
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public boolean remove(Object o) {
      if (inRange((T) o)) {
        return remove(o);
      } else {
        return false;
      }
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
      if (end == null) {
        return new SubTreeIterator(start, end);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      return !iterator().hasNext();
    }

    public Comparator<? super T> comparator() {
      return null;
    }

    public SortedSet<T> subSet(T fromElement, T toElement) {
      if (inRangeInclusive(fromElement) && inRangeInclusive(toElement)) {
        return new SubSet(fromElement, toElement);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public SortedSet<T> headSet(T toElement) {
      if (inRangeInclusive(toElement)) {
        return new SubSet(start, toElement);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public SortedSet<T> tailSet(T fromElement) {
      if (inRangeInclusive(fromElement)) {
        return new SubSet(fromElement, end);
      } else {
        throw new IllegalArgumentException();
      }
    }

    public T first() {
      if (start == null) {
        return AATreeSet.this.first();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    public T last() {
      if (end == null) {
        return AATreeSet.this.last();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    private boolean inRange(T value) {
      return (start == null || start.compareTo(value) <= 0) && (end == null || end.compareTo(value) > 0);
    }

    private boolean inRangeInclusive(T value) {
      return (start == null || start.compareTo(value) <= 0) && (end == null || end.compareTo(value) >= 0);
    }
  }

  /**
   * Iterator that iterates over the tree.
   */
  private class TreeIterator implements Iterator<T> {

    private final Stack<Node<T>> path = new Stack<Node<T>>();
    private Node<T> next;

    TreeIterator() {
      path.push(terminal());
      Node<T> leftMost = root;
      while (leftMost.getLeft() != terminal()) {
        path.push(leftMost);
        leftMost = leftMost.getLeft();
      }
      next = leftMost;
    }

    TreeIterator(T start) {
      path.push(terminal());
      Node<T> current = root;
      while (true) {
        int direction = current.compareTo(start);
        if (direction > 0) {
          if (current.getLeft() == terminal()) {
            next = current;
            break;
          } else {
            path.push(current);
            current = current.getLeft();
          }
        } else if (direction < 0) {
          if (current.getRight() == terminal()) {
            next = path.pop();
            break;
          } else {
            current = current.getRight();
          }
        } else {
          next = current;
          break;
        }
      }
    }

    public boolean hasNext() {
      return next != terminal();
    }

    public T next() {
      Node<T> current = next;
      advance();
      return current.getPayload();
    }

    private void advance() {
      Node<T> successor = next.getRight();
      if (successor != terminal()) {
        while (successor.getLeft() != terminal()) {
          path.push(successor);
          successor = successor.getLeft();
        }
        next = successor;
      } else {
        next = path.pop();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * Iterator that iterates over a subset of the tree.
   */
  private class SubTreeIterator extends TreeIterator {
    SubTreeIterator(T start, T end) {
      super(start);
    }
  }
}
