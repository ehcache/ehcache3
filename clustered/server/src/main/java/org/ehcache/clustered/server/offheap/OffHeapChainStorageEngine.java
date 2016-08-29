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
package org.ehcache.clustered.server.offheap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;

import static java.util.Collections.unmodifiableList;

class OffHeapChainStorageEngine<K> implements StorageEngine<K, InternalChain> {

  private static final int ELEMENT_HEADER_SEQUENCE_OFFSET = 0;
  private static final int ELEMENT_HEADER_LENGTH_OFFSET = 8;
  private static final int ELEMENT_HEADER_NEXT_OFFSET = 12;
  private static final int ELEMENT_HEADER_SIZE = 20;

  private static final int CHAIN_HEADER_KEY_LENGTH_OFFSET = 0;
  private static final int CHAIN_HEADER_KEY_HASH_OFFSET = 4;
  private static final int CHAIN_HEADER_TAIL_OFFSET = 8;
  private static final int CHAIN_HEADER_SIZE = 16;

  private final OffHeapStorageArea storage;
  private final Portability<? super K> keyPortability;
  private final Set<AttachedInternalChain> activeChains = new HashSet<AttachedInternalChain>();

  private StorageEngine.Owner owner;
  private long nextSequenceNumber = 0;

  public OffHeapChainStorageEngine(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean thief, boolean victim) {
    this.storage = new OffHeapStorageArea(PointerSize.LONG, new StorageOwner(), source, minPageSize, maxPageSize, thief, victim);
    this.keyPortability = keyPortability;
  }

  InternalChain newChain(ByteBuffer element) {
    return new PrimordialChain(element);
  }

  @Override
  public Long writeMapping(K key, InternalChain value, int hash, int metadata) {
    if (value instanceof PrimordialChain) {
      return createAttachedChain(key, hash, (PrimordialChain) value);
    } else {
      throw new AssertionError("only detached internal chains should be initially written");
    }
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    //nothing
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    AttachedInternalChain chain = new AttachedInternalChain(encoding);
    try {
      chain.free();
    } finally {
      chain.close();
    }
  }

  @Override
  public InternalChain readValue(long encoding) {
    return new AttachedInternalChain(encoding);
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    AttachedInternalChain chain = new AttachedInternalChain(encoding);
    try {
      return chain.equals(value);
    } finally {
      chain.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public K readKey(long encoding, int hashCode) {
    return (K) keyPortability.decode(readKeyBuffer(encoding));
  }

  @Override
  public boolean equalsKey(Object key, long encoding) {
    return keyPortability.equals(key, readKeyBuffer(encoding));
  }

  private ByteBuffer readKeyBuffer(long encoding) {
    int keyLength = readKeySize(encoding);
    int elemLength = storage.readInt(encoding + CHAIN_HEADER_SIZE + ELEMENT_HEADER_LENGTH_OFFSET);
    return storage.readBuffer(encoding + CHAIN_HEADER_SIZE + ELEMENT_HEADER_SIZE + elemLength, keyLength);
  }

  private int readKeyHash(long encoding) {
    return storage.readInt(encoding + CHAIN_HEADER_KEY_HASH_OFFSET);
  }

  private int readKeySize(long encoding) {
    return Integer.MAX_VALUE & storage.readInt(encoding + CHAIN_HEADER_KEY_LENGTH_OFFSET);
  }

  @Override
  public void clear() {
    storage.clear();
  }

  @Override
  public long getAllocatedMemory() {
    return storage.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return storage.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return getOccupiedMemory();
  }

  @Override
  public long getDataSize() {
    return storage.getAllocatedMemory();
  }

  @Override
  public void invalidateCache() {
    //no-op - for now
  }

  @Override
  public void bind(StorageEngine.Owner owner) {
    this.owner = owner;
  }

  @Override
  public void destroy() {
    storage.destroy();
  }

  @Override
  public boolean shrink() {
    return storage.shrink();
  }

  private static class DetachedChain implements Chain {

    private final List<Element> elements;

    private DetachedChain(List<Element> buffers) {
      this.elements = unmodifiableList(buffers);
    }

    @Override
    public Iterator<Element> reverseIterator() {
      return Util.reverseIterator(elements);
    }

    @Override
    public boolean isEmpty() {
      return elements.isEmpty();
    }

    @Override
    public Iterator<Element> iterator() {
      return elements.iterator();
    }

  }

  private static class PrimordialChain implements InternalChain {

    private final ByteBuffer element;

    public PrimordialChain(ByteBuffer element) {
      this.element = element;
    }

    @Override
    public Chain detach() {
      throw new AssertionError("primordial chains cannot be detached");
    }

    @Override
    public boolean append(ByteBuffer element) {
      throw new AssertionError("primordial chains cannot be appended");
    }

    @Override
    public boolean replace(Chain expected, Chain replacement) {
      throw new AssertionError("primordial chains cannot be mutated");
    }

    @Override
    public void close() {
      //no-op
    }
  }

  private final class AttachedInternalChain implements InternalChain {

    /**
     * Location of the chain structure, not of the first element.
     */
    private long chain;

    AttachedInternalChain(long address) {
      this.chain = address;
      OffHeapChainStorageEngine.this.activeChains.add(this);
    }

    @Override
    public Chain detach() {
      List<Element> buffers = new ArrayList<Element>();

      long element = chain + CHAIN_HEADER_SIZE;
      do {
        buffers.add(element(readElementBuffer(element), readElementSequenceNumber(element)));
        element = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
      } while (element != chain);

      return new DetachedChain(buffers);
    }

    @Override
    public boolean append(ByteBuffer element) {
      long newTail = createElement(element);
      if (newTail < 0) {
        return false;
      } else {
        long oldTail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);
        storage.writeLong(newTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
        try {
          storage.writeLong(oldTail + ELEMENT_HEADER_NEXT_OFFSET, newTail);
        } catch (NullPointerException e) {
          throw e;
        }
        storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, newTail);
        return true;
      }
    }

    @Override
    public boolean replace(Chain expected, Chain replacement) {
      if (expected.isEmpty()) {
        throw new IllegalArgumentException("Empty expected sequence");
      } else if (replacement.isEmpty()) {
        return removeHeader(expected);
      } else {
        return replaceHeader(expected, replacement);
      }
    }

    public boolean removeHeader(Chain header) {
      long suffixHead = chain + CHAIN_HEADER_SIZE;
      long prefixTail;

      Iterator<Element> iterator = header.iterator();
      do {
        if (!compare(iterator.next(), suffixHead)) {
          return true;
        }
        prefixTail = suffixHead;
        suffixHead = storage.readLong(suffixHead + ELEMENT_HEADER_NEXT_OFFSET);
      } while (iterator.hasNext());

      if (suffixHead == chain) {
        //whole chain removed
        int slot = owner.getSlotForHashAndEncoding(readKeyHash(chain), chain, ~0);
        if (!owner.evict(slot, true)) {
          throw new AssertionError("Unexpected failure to evict slot " + slot);
        }
        return true;
      } else {
        int hash = readKeyHash(chain);
        int elemSize = storage.readInt(suffixHead + ELEMENT_HEADER_LENGTH_OFFSET);
        ByteBuffer elemBuffer = storage.readBuffer(suffixHead + ELEMENT_HEADER_SIZE, elemSize);
        Long newChainAddress = createAttachedChain(readKeyBuffer(chain), hash, elemBuffer);
        if (newChainAddress == null) {
          return false;
        } else {
          AttachedInternalChain newChain = new AttachedInternalChain(newChainAddress);
          try {
            //copy remaining elements from old chain (by reference)
            long next = storage.readLong(suffixHead + ELEMENT_HEADER_NEXT_OFFSET);
            long tail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);
            if (next != chain) {
              newChain.append(next, tail);
            }

            if (owner.updateEncoding(hash, chain, newChainAddress, ~0)) {
              storage.writeLong(prefixTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
              free();
              return true;
            } else {
              newChain.free();
              throw new AssertionError("Encoding update failure - impossible!");
            }
          } finally {
            newChain.close();
          }
        }
      }
    }

    public boolean replaceHeader(Chain expected, Chain replacement) {
      long suffixHead = chain + CHAIN_HEADER_SIZE;
      long prefixTail;

      Iterator<Element> expectedIt = expected.iterator();
      do {
        if (!compare(expectedIt.next(), suffixHead)) {
          return true;
        }
        prefixTail = suffixHead;
        suffixHead = storage.readLong(suffixHead + ELEMENT_HEADER_NEXT_OFFSET);
      } while (expectedIt.hasNext());

      int hash = readKeyHash(chain);
      Long newChainAddress = createAttachedChain(readKeyBuffer(chain), hash, replacement);
      if (newChainAddress == null) {
        return false;
      } else {
        AttachedInternalChain newChain = new AttachedInternalChain(newChainAddress);
        try {
          //copy remaining elements from old chain (by reference)
          if (suffixHead != chain) {
            newChain.append(suffixHead, storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET));
          }

          if (owner.updateEncoding(hash, chain, newChainAddress, ~0)) {
            storage.writeLong(prefixTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
            free();
            return true;
          } else {
            newChain.free();
            throw new AssertionError("Encoding update failure - impossible!");
          }
        } finally {
          newChain.close();
        }
      }
    }

    private void free() {
      long element = storage.readLong(chain + CHAIN_HEADER_SIZE + ELEMENT_HEADER_NEXT_OFFSET);
      storage.free(chain);

      while (element != chain) {
        long next = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
        storage.free(element);
        element = next;
      }
    }

    private long createElement(ByteBuffer element) {
      long newElement = storage.allocate(element.remaining() + ELEMENT_HEADER_SIZE);
      if (newElement < 0) {
        return newElement;
      } else {
        writeElement(newElement, element);
        return newElement;
      }
    }

    private boolean compare(Element element, long address) {
      if (element instanceof SequencedElement) {
        return readElementSequenceNumber(address) == ((SequencedElement) element).getSequenceNumber();
      } else {
        return readElementBuffer(address).equals(element.getPayload());
      }
    }

    private void append(long head, long tail) {
      long oldTail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);

      storage.writeLong(oldTail + ELEMENT_HEADER_NEXT_OFFSET, head);
      storage.writeLong(tail + ELEMENT_HEADER_NEXT_OFFSET, chain);
      storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, tail);
    }

    private Element element(ByteBuffer attachedBuffer, final long sequence) {
      final ByteBuffer detachedBuffer = (ByteBuffer) ByteBuffer.allocate(attachedBuffer.remaining()).put(attachedBuffer).flip();

      return new SequencedElement() {

        @Override
        public ByteBuffer getPayload() {
          return detachedBuffer.asReadOnlyBuffer();
        }

        @Override
        public long getSequenceNumber() {
          return sequence;
        }
      };
    }

    private ByteBuffer readElementBuffer(long address) {
      int elemLength = storage.readInt(address + ELEMENT_HEADER_LENGTH_OFFSET);
      return storage.readBuffer(address + ELEMENT_HEADER_SIZE, elemLength);
    }

    private long readElementSequenceNumber(long address) {
      return storage.readLong(address + ELEMENT_HEADER_SEQUENCE_OFFSET);
    }

    public void moved(long from, long to) {
      if (from == chain) {
        chain = to;
      }
    }

    @Override
    public void close() {
      OffHeapChainStorageEngine.this.activeChains.remove(this);
    }
  }

  private long writeElement(long address, ByteBuffer element) {
    storage.writeLong(address + ELEMENT_HEADER_SEQUENCE_OFFSET, nextSequenceNumber++);
    storage.writeInt(address + ELEMENT_HEADER_LENGTH_OFFSET, element.remaining());
    storage.writeBuffer(address + ELEMENT_HEADER_SIZE, element.duplicate());
    return address;
  }

  private Long createAttachedChain(K key, int hash, PrimordialChain value) {
    ByteBuffer keyBuffer = keyPortability.encode(key);
    ByteBuffer elemBuffer = value.element;
    return createAttachedChain(keyBuffer, hash, elemBuffer);
  }

  private Long createAttachedChain(ByteBuffer keyBuffer, int hash, ByteBuffer elemBuffer) {
    long chain = storage.allocate(keyBuffer.remaining() + elemBuffer.remaining() + CHAIN_HEADER_SIZE + ELEMENT_HEADER_SIZE);
    if (chain < 0) {
      return null;
    }
    int keySize = keyBuffer.remaining();
    storage.writeInt(chain + CHAIN_HEADER_KEY_HASH_OFFSET, hash);
    storage.writeInt(chain + CHAIN_HEADER_KEY_LENGTH_OFFSET, Integer.MIN_VALUE | keySize);
    storage.writeBuffer(chain + CHAIN_HEADER_SIZE + ELEMENT_HEADER_SIZE + elemBuffer.remaining(), keyBuffer);
    long element = chain + CHAIN_HEADER_SIZE;
    writeElement(element, elemBuffer);
    storage.writeLong(element + ELEMENT_HEADER_NEXT_OFFSET, chain);
    storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, element);
    return chain;
  }

  private Long createAttachedChain(ByteBuffer readKeyBuffer, int hash, Chain from) {
    Iterator<Element> iterator = from.iterator();
    Long address = createAttachedChain(readKeyBuffer, hash, iterator.next().getPayload());
    if (address == null) {
      return null;
    }

    AttachedInternalChain chain = new AttachedInternalChain(address);
    try {
      while (iterator.hasNext()) {
        if (!chain.append(iterator.next().getPayload())) {
          chain.free();
          return null;
        }
      }
    } finally {
      chain.close();
    }
    return address;
  }

  private long findHead(long address) {
    while (!isHead(address)) {
      address = storage.readLong(address + ELEMENT_HEADER_NEXT_OFFSET);
    }
    return address;
  }

  private boolean isHead(long address) {
    return storage.readInt(address + CHAIN_HEADER_KEY_LENGTH_OFFSET) < 0;
  }

  class StorageOwner implements OffHeapStorageArea.Owner {

    @Override
    public boolean evictAtAddress(long address, boolean shrink) {
      long chain = findHead(address);
      for (AttachedInternalChain activeChain : activeChains) {
        if (activeChain.chain == chain) {
          return false;
        }
      }
      int hash = storage.readInt(chain + CHAIN_HEADER_KEY_HASH_OFFSET);
      int slot = owner.getSlotForHashAndEncoding(hash, chain, ~0);
      return owner.evict(slot, shrink);
    }

    @Override
    public Lock writeLock() {
      return owner.writeLock();
    }

    @Override
    public boolean isThief() {
      return owner.isThiefForTableAllocations();
    }

    @Override
    public boolean moved(long from, long to) {
      if (isHead(to)) {
        int hashCode = storage.readInt(to + CHAIN_HEADER_KEY_HASH_OFFSET);
        if (!owner.updateEncoding(hashCode, from, to, ~0)) {
          return false;
        } else {
          long tail = storage.readLong(to + CHAIN_HEADER_TAIL_OFFSET);
          if (tail == from + CHAIN_HEADER_SIZE) {
            tail = to + CHAIN_HEADER_SIZE;
            storage.writeLong(to + CHAIN_HEADER_TAIL_OFFSET, tail);
          }
          storage.writeLong(tail + ELEMENT_HEADER_NEXT_OFFSET, to);
          for (AttachedInternalChain activeChain : activeChains) {
            activeChain.moved(from, to);
          }
          return true;
        }
      } else {
        long chain = findHead(to);

        long tail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);
        if (tail == from) {
          storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, to);
        }

        long element = chain + CHAIN_HEADER_SIZE;
        while (element != chain) {
          long next = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
          if (next == from) {
            storage.writeLong(element + ELEMENT_HEADER_NEXT_OFFSET, to);
            return true;
          } else {
            element = next;
          }
        }
        throw new AssertionError();
      }
    }

    @Override
    public int sizeOf(long address) {
      if (isHead(address)) {
        int keySize = readKeySize(address);
        return CHAIN_HEADER_SIZE + keySize + sizeOf(address + CHAIN_HEADER_SIZE);
      } else {
        int elementSize = storage.readInt(address + ELEMENT_HEADER_LENGTH_OFFSET);
        return ELEMENT_HEADER_SIZE + elementSize;
      }
    }
  }
}
