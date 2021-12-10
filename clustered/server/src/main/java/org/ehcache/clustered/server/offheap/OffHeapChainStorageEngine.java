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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.BinaryStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.WriteContext;
import org.terracotta.offheapstore.util.Factory;

import static java.util.Collections.unmodifiableList;
import static org.ehcache.clustered.common.internal.util.ChainBuilder.chainFromList;

public class OffHeapChainStorageEngine<K> implements ChainStorageEngine<K>, BinaryStorageEngine {
  private static final int ELEMENT_HEADER_SEQUENCE_OFFSET = 0;
  private static final int ELEMENT_HEADER_LENGTH_OFFSET = 8;
  private static final int ELEMENT_HEADER_NEXT_OFFSET = 12;
  private static final int ELEMENT_HEADER_SIZE = 20;

  private static final int CHAIN_HEADER_KEY_LENGTH_OFFSET = 0;
  private static final int CHAIN_HEADER_KEY_HASH_OFFSET = 4;
  private static final int CHAIN_HEADER_TAIL_OFFSET = 8;
  private static final int CHAIN_HEADER_SIZE = 16;

  private static final int DETACHED_CONTIGUOUS_CHAIN_ADDRESS_OFFSET = 0;
  private static final int DETACHED_CONTIGUOUS_CHAIN_HEADER_SIZE = 8;

  private final OffHeapStorageArea storage;
  private final Portability<? super K> keyPortability;
  private final Set<AttachedInternalChain> activeChains = Collections.newSetFromMap(new ConcurrentHashMap<AttachedInternalChain, Boolean>());
  private final int extendedChainHeaderSize;
  private final ByteBuffer emptyExtendedChainHeader;
  private final int totalChainHeaderSize;

  protected StorageEngine.Owner owner;
  private long nextSequenceNumber = 0;
  private volatile boolean hasContiguousChains = false;

  public static <K> Factory<? extends ChainStorageEngine<K>>
  createFactory(final PageSource source,
                final Portability<? super K> keyPortability,
                final int minPageSize, final int maxPageSize,
                final boolean thief, final boolean victim) {
    return (Factory<OffHeapChainStorageEngine<K>>)() -> new OffHeapChainStorageEngine<>(source, keyPortability,
        minPageSize, maxPageSize, thief, victim);
  }

  OffHeapChainStorageEngine(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean thief, boolean victim) {
    this(source, keyPortability, minPageSize, maxPageSize, thief, victim, ByteBuffer.allocate(0));
  }

  protected OffHeapChainStorageEngine(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean thief, boolean victim,
                                      final ByteBuffer emptyExtendedChainHeader) {
    this.storage = new OffHeapStorageArea(PointerSize.LONG, new StorageOwner(), source, minPageSize, maxPageSize, thief, victim);
    this.keyPortability = keyPortability;
    this.extendedChainHeaderSize = emptyExtendedChainHeader.remaining();
    this.emptyExtendedChainHeader = emptyExtendedChainHeader;
    this.totalChainHeaderSize = CHAIN_HEADER_SIZE + this.extendedChainHeaderSize;
  }

  //For tests
  Set<AttachedInternalChain> getActiveChains() {
    return this.activeChains;
  }

  @Override
  public InternalChain newChain(ByteBuffer element) {
    return new GenesisLink(element);
  }

  @Override
  public InternalChain newChain(Chain chain) {
    return new GenesisLinks(chain);
  }

  @Override
  public Long writeMapping(K key, InternalChain value, int hash, int metadata) {
    if (value instanceof GenesisChain) {
      return createAttachedChain(key, hash, (GenesisChain) value);
    } else {
      throw new AssertionError("only detached internal chains should be initially written");
    }
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    chainAttached(encoding);
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    try (AttachedInternalChain chain = new AttachedInternalChain(encoding)) {
      chain.free();
    }
  }

  @Override
  public InternalChain readValue(long encoding) {
    return new AttachedInternalChain(encoding);
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    try (AttachedInternalChain chain = new AttachedInternalChain(encoding)) {
      return chain.equals(value);
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
    int elemLength = readElementLength(encoding + this.totalChainHeaderSize);
    return storage.readBuffer(encoding + this.totalChainHeaderSize + ELEMENT_HEADER_SIZE + elemLength, keyLength);
  }

  @Override
  public int readKeyHash(long encoding) {
    return storage.readInt(encoding + CHAIN_HEADER_KEY_HASH_OFFSET);
  }

  private int readElementLength(long element) {
    // The most significant bit (MSB) of element length is used to signify whether an element is explicitly allocated
    // (msb clear) or part of a contiguous chain (msb set). Clear the msb when returning length.
    return Integer.MAX_VALUE & storage.readInt(element + ELEMENT_HEADER_LENGTH_OFFSET);
  }

  @Override
  public ByteBuffer readBinaryKey(long encoding) {
    return readKeyBuffer(encoding);
  }

  @Override
  public ByteBuffer readBinaryValue(long chain) {
    // first get total element size and allocate buffer
    long element = chain + this.totalChainHeaderSize;
    int totalLength = DETACHED_CONTIGUOUS_CHAIN_HEADER_SIZE;
    do {
      totalLength += ELEMENT_HEADER_SIZE + readElementLength(element);
      element = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
    } while (element != chain);

    final ByteBuffer detachedContiguousBuffer = ByteBuffer.allocate(totalLength);
    // one way for layers above to extract encoding is to put the encoding of the chain address in the value
    detachedContiguousBuffer.putLong(chain);

    // now add the elements to the buffer
    element = chain + this.totalChainHeaderSize;
    do {
      final int startPosition = detachedContiguousBuffer.position();
      detachedContiguousBuffer.put(storage.readBuffer(element, ELEMENT_HEADER_SIZE + readElementLength(element)));
      detachedContiguousBuffer.mark();
      detachedContiguousBuffer.putLong(startPosition + ELEMENT_HEADER_NEXT_OFFSET, -1L);
      detachedContiguousBuffer.reset();
      element = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
    } while (element != chain);
    return (ByteBuffer)detachedContiguousBuffer.flip();
  }

  @Override
  public boolean equalsBinaryKey(ByteBuffer binaryKey, long chain) {
    return binaryKey.equals(readKeyBuffer(chain));
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata) {
    final int totalSize = binaryKey.remaining() +
                          (binaryValue.remaining() - DETACHED_CONTIGUOUS_CHAIN_HEADER_SIZE)
                          + this.totalChainHeaderSize;
    long chain = storage.allocate(totalSize);
    if (chain < 0) {
      return null;
    }
    if (binaryValue.remaining() < DETACHED_CONTIGUOUS_CHAIN_HEADER_SIZE + ELEMENT_HEADER_SIZE) {
      // a chain must have at least one element. Something is wrong
      throw new AssertionError("Invalid chain data detected. Empty links");
    }
    binaryValue.mark();
    binaryKey.mark();
    try {
      // extract first element
      binaryValue.position(DETACHED_CONTIGUOUS_CHAIN_HEADER_SIZE);
      final ByteBuffer firstElementWithHeader = binaryValue.slice();
      final int firstElementWithHeaderSize = ELEMENT_HEADER_SIZE +
                                             (Integer.MAX_VALUE & firstElementWithHeader.getInt(ELEMENT_HEADER_LENGTH_OFFSET));
      firstElementWithHeader.limit(firstElementWithHeaderSize);
      binaryValue.position(binaryValue.position() + firstElementWithHeaderSize);

      // mark relevant locations
      final int keySize = binaryKey.remaining();
      final long firstElementLocation = chain + this.totalChainHeaderSize;
      final long keyLocation = firstElementLocation + firstElementWithHeaderSize;
      final long restOfElementsLocation = keyLocation + keySize;

      // build element length list
      final ByteBuffer restOfElementsBuffer = binaryValue.slice();
      final List<Integer> restOfElementLengthsWithHeader = new ArrayList<>();
      while (restOfElementsBuffer.hasRemaining()) {
        final int skipLength = ELEMENT_HEADER_SIZE + (Integer.MAX_VALUE & restOfElementsBuffer.getInt(
            restOfElementsBuffer.position() + ELEMENT_HEADER_LENGTH_OFFSET));
        restOfElementLengthsWithHeader.add(skipLength);
        restOfElementsBuffer.position(restOfElementsBuffer.position() + skipLength);
      }
      restOfElementsBuffer.rewind();

      // now write all the data
      storage.writeInt(chain + CHAIN_HEADER_KEY_HASH_OFFSET, hash);
      storage.writeInt(chain + CHAIN_HEADER_KEY_LENGTH_OFFSET, Integer.MIN_VALUE | keySize);
      storage.writeBuffer(keyLocation, binaryKey);
      storage.writeBuffer(firstElementLocation, firstElementWithHeader);
      storage.writeBuffer(chain + CHAIN_HEADER_SIZE, emptyExtendedChainHeader.duplicate());
      if (restOfElementsBuffer.hasRemaining()) {
        storage.writeBuffer(restOfElementsLocation, restOfElementsBuffer);
      }

      // now adjust offsets
      if (restOfElementLengthsWithHeader.size() <= 0) {
        // we have only one element
        storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, firstElementLocation);
        storage.writeLong(firstElementLocation + ELEMENT_HEADER_NEXT_OFFSET, chain);
      } else {
        // recovering the buffer into a contiguous chain..denote this..
        this.hasContiguousChains = true;
        storage.writeLong(firstElementLocation + ELEMENT_HEADER_NEXT_OFFSET, restOfElementsLocation);
        long currentLocation = restOfElementsLocation;
        int i = 0;
        for (; i < restOfElementLengthsWithHeader.size() - 1; i++) {
          final int elemLength = restOfElementLengthsWithHeader.get(i) - ELEMENT_HEADER_SIZE;
          final int adjustedLength = Integer.MIN_VALUE | elemLength;
          long nextLocation = currentLocation + elemLength + ELEMENT_HEADER_SIZE;
          storage.writeLong(currentLocation + ELEMENT_HEADER_NEXT_OFFSET, nextLocation);
          // denote that this is not an allocated chunk
          storage.writeInt(currentLocation + ELEMENT_HEADER_LENGTH_OFFSET, adjustedLength);
          currentLocation = nextLocation;
        }
        final int adjustedLength = Integer.MIN_VALUE | (restOfElementLengthsWithHeader.get(i) - ELEMENT_HEADER_SIZE);
        storage.writeLong(currentLocation + ELEMENT_HEADER_NEXT_OFFSET, chain);
        storage.writeInt(currentLocation + ELEMENT_HEADER_LENGTH_OFFSET, adjustedLength);
        storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, currentLocation);
      }
      return chain;
    } finally {
      binaryKey.reset();
      binaryValue.reset();
    }
  }

  public static long extractChainAddressFromValue(ByteBuffer valueBuffer) {
    return valueBuffer.getLong(DETACHED_CONTIGUOUS_CHAIN_ADDRESS_OFFSET);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] byteBuffers, ByteBuffer[] byteBuffers1, int i, int i1) {
    throw new AssertionError("Operation Not supported");
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

  protected ByteBuffer getExtensionHeader(long chainAddress) {
    checkExtensionHeaderExists();
    return storage.readBuffer(toExtensionAddress(chainAddress), extendedChainHeaderSize);
  }

  protected WriteContext getExtensionWriteContext(long chainAddress) {
    checkExtensionHeaderExists();
    return new WriteContext() {

      @Override
      public void setLong(int offset, long value) {
        if (offset < 0 || offset >= extendedChainHeaderSize) {
          throw new IllegalArgumentException("Offset not within bounds 0 >= " + offset + " < " + extendedChainHeaderSize);
        } else {
          storage.writeLong(toExtensionAddress(chainAddress) + offset, value);
        }
      }

      @Override
      public void flush() {
        //no-op
      }
    };
  }

  protected void chainAttached(long chainAddress) {
  }

  protected void chainFreed(long chainAddress) {
  }

  protected void chainModified(long chainAddress) {
  }

  protected void chainMoved(long fromChainAddress, long toChainAddress) {
  }

  private void checkExtensionHeaderExists() {
    if (extendedChainHeaderSize <= 0) {
      throw new AssertionError("No extended header support for this storage engine");
    }
  }

  private long toExtensionAddress(long chainAddress) {
    return chainAddress + CHAIN_HEADER_SIZE;
  }

  /**
   * Represents the initial form of a chain before the storage engine writes the chain mapping
   * to the underlying map against the key.
   */
  private static abstract class GenesisChain implements InternalChain {
    @Override
    public Chain detach() {
      throw new AssertionError("Chain not in storage yet. Cannot be detached");
    }

    @Override
    public boolean append(ByteBuffer element) {
      throw new AssertionError("Chain not in storage yet. Cannot be appended");
    }

    @Override
    public boolean replace(Chain expected, Chain replacement) {
      throw new AssertionError("Chain not in storage yet. Cannot be mutated");
    }

    @Override
    public void close() {
      //no-op
    }

    protected abstract Iterator<Element> iterator();
  }

  /**
   * Represents a simple {@link GenesisChain} that contains a single link.
   */
  private static class GenesisLink extends GenesisChain {
    private final Element element;

    public GenesisLink(ByteBuffer buffer) {
      element = buffer::asReadOnlyBuffer;
    }

    @Override
    protected Iterator<Element> iterator() {
      return Collections.singleton(element).iterator();
    }
  }

  /**
   * Represents a more complex {@link GenesisChain} that contains multiple links represented itself
   * as a {@link Chain}.
   */
  private static class GenesisLinks extends GenesisChain {
    private final Chain chain;

    public GenesisLinks(Chain chain) {
      this.chain = chain;
    }

    @Override
    protected Iterator<Element> iterator() {
      return chain.iterator();
    }
  }

  private final class AttachedInternalChain implements InternalChain {

    /**
     * Location of the chain structure, not of the first element.
     */
    private long chain;
    /**
     * track if this chain is modified so that we can signal on close
     */
    private boolean chainModified = false;

    AttachedInternalChain(long address) {
      this.chain = address;
      OffHeapChainStorageEngine.this.activeChains.add(this);
    }

    @Override
    public Chain detach() {
      List<Element> buffers = new ArrayList<>();

      long element = chain + OffHeapChainStorageEngine.this.totalChainHeaderSize;
      do {
        buffers.add(element(readElementBuffer(element), readElementSequenceNumber(element)));
        element = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
      } while (element != chain);

      return chainFromList(buffers);
    }

    @Override
    public boolean append(ByteBuffer element) {
      long newTail = createElement(element);
      if (newTail < 0) {
        return false;
      } else {
        this.chainModified = true;
        long oldTail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);
        storage.writeLong(newTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
        storage.writeLong(oldTail + ELEMENT_HEADER_NEXT_OFFSET, newTail);
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


    /**
     * @return false if storage can't be allocated for new header when whole chain is not removed, true otherwise
     */
    public boolean removeHeader(Chain header) {
      long suffixHead = chain + OffHeapChainStorageEngine.this.totalChainHeaderSize;
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
        int elemSize = readElementLength(suffixHead);
        ByteBuffer elemBuffer = storage.readBuffer(suffixHead + ELEMENT_HEADER_SIZE, elemSize);
        Long newChainAddress = createAttachedChain(readKeyBuffer(chain), hash, elemBuffer);
        if (newChainAddress == null) {
          return false;
        } else {
          try (AttachedInternalChain newChain = new AttachedInternalChain(newChainAddress)) {
            newChain.chainModified = true;
            //copy remaining elements from old chain (by reference)
            long next = storage.readLong(suffixHead + ELEMENT_HEADER_NEXT_OFFSET);
            long tail = storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET);
            if (next != chain) {
              newChain.append(next, tail);
            }

            if (owner.updateEncoding(hash, chain, newChainAddress, ~0)) {
              storage.writeLong(prefixTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
              chainMoved(chain, newChainAddress);
              free();
              return true;
            } else {
              newChain.free();
              throw new AssertionError("Encoding update failure - impossible!");
            }
          }
        }
      }
    }

    /**
     * @return false if storage can't be allocated for new header when head of the current chain matches expected
     * chain, true otherwise
     */
    public boolean replaceHeader(Chain expected, Chain replacement) {
      long suffixHead = chain + OffHeapChainStorageEngine.this.totalChainHeaderSize;
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
      Long newChainAddress = createAttachedChain(readKeyBuffer(chain), hash, replacement.iterator());
      if (newChainAddress == null) {
        return false;
      } else {
        try (AttachedInternalChain newChain = new AttachedInternalChain(newChainAddress)) {
          newChain.chainModified = true;
          //copy remaining elements from old chain (by reference)
          if (suffixHead != chain) {
            newChain.append(suffixHead, storage.readLong(chain + CHAIN_HEADER_TAIL_OFFSET));
          }

          if (owner.updateEncoding(hash, chain, newChainAddress, ~0)) {
            storage.writeLong(prefixTail + ELEMENT_HEADER_NEXT_OFFSET, chain);
            chainMoved(chain, newChainAddress);
            free();
            return true;
          } else {
            newChain.free();
            throw new AssertionError("Encoding update failure - impossible!");
          }
        }
      }
    }

    private void free() {
      // signal dependent engines to act on this free before freeing the storage
      chainFreed(chain);
      chainModified = false;

      long element = storage.readLong(chain + OffHeapChainStorageEngine.this.totalChainHeaderSize + ELEMENT_HEADER_NEXT_OFFSET);
      while (element != chain) {
        long next = storage.readLong(element + ELEMENT_HEADER_NEXT_OFFSET);
        if (storage.readInt(element + ELEMENT_HEADER_LENGTH_OFFSET) >= 0) {
          // do not free blocks contiguous to chain
          storage.free(element);
        }
        element = next;
      }

      storage.free(chain);
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

      if (OffHeapChainStorageEngine.this.hasContiguousChains) {
        // we will have to move out any contiguous elements in the old chain as it is going to be freed soon
        long current = head;
        long prev = oldTail;
        while (current != chain) {
          final long next = storage.readLong(current + ELEMENT_HEADER_NEXT_OFFSET);
          final int elemLength = storage.readInt(current + ELEMENT_HEADER_LENGTH_OFFSET);
          if (elemLength < 0) {
            final int elemLengthWithHeader = (Integer.MAX_VALUE & elemLength) + ELEMENT_HEADER_SIZE;
            final long element = storage.allocate(elemLengthWithHeader);
            storage.writeBuffer(element, storage.readBuffer(current, elemLengthWithHeader));
            storage.writeInt(element + ELEMENT_HEADER_LENGTH_OFFSET, elemLengthWithHeader - ELEMENT_HEADER_SIZE);
            storage.writeLong(prev + ELEMENT_HEADER_NEXT_OFFSET, element);
            prev = element;
          } else {
            prev = current;
          }
          current = next;
        }
        storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, prev);
      }
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
      int elemLength = readElementLength(address);
      return storage.readBuffer(address + ELEMENT_HEADER_SIZE, elemLength);
    }

    private long readElementSequenceNumber(long address) {
      return storage.readLong(address + ELEMENT_HEADER_SEQUENCE_OFFSET);
    }

    public void moved(long from, long to) {
      if (from == chain) {
        chain = to;
        if (from != to) {
          chainMoved(from, to);
        }
      }
    }

    @Override
    public void close() {
      try {
        if (this.chainModified) {
          this.chainModified = false;
          chainModified(chain);
        }
      } finally {
        // must remove even if chain modified threw an unexpected exception
        OffHeapChainStorageEngine.this.activeChains.remove(this);
      }
    }
  }

  private long writeElement(long address, ByteBuffer element) {
    storage.writeLong(address + ELEMENT_HEADER_SEQUENCE_OFFSET, nextSequenceNumber++);
    storage.writeInt(address + ELEMENT_HEADER_LENGTH_OFFSET, element.remaining());
    storage.writeBuffer(address + ELEMENT_HEADER_SIZE, element.duplicate());
    return address;
  }

  private Long createAttachedChain(K key, int hash, GenesisChain value) {
    ByteBuffer keyBuffer = keyPortability.encode(key);
    return createAttachedChain(keyBuffer, hash, value.iterator());
  }

  private Long createAttachedChain(ByteBuffer keyBuffer, int hash, ByteBuffer elemBuffer) {
    long chain = storage.allocate(keyBuffer.remaining() + elemBuffer.remaining() + this.totalChainHeaderSize + ELEMENT_HEADER_SIZE);
    if (chain < 0) {
      return null;
    }
    int keySize = keyBuffer.remaining();
    storage.writeInt(chain + CHAIN_HEADER_KEY_HASH_OFFSET, hash);
    storage.writeInt(chain + CHAIN_HEADER_KEY_LENGTH_OFFSET, Integer.MIN_VALUE | keySize);
    storage.writeBuffer(chain + this.totalChainHeaderSize + ELEMENT_HEADER_SIZE + elemBuffer.remaining(), keyBuffer);
    if (extendedChainHeaderSize > 0) {
      storage.writeBuffer(chain + CHAIN_HEADER_SIZE, emptyExtendedChainHeader.duplicate());
    }
    long element = chain + this.totalChainHeaderSize;
    writeElement(element, elemBuffer);
    storage.writeLong(element + ELEMENT_HEADER_NEXT_OFFSET, chain);
    storage.writeLong(chain + CHAIN_HEADER_TAIL_OFFSET, element);
    return chain;
  }

  private Long createAttachedChain(ByteBuffer readKeyBuffer, int hash, Iterator<Element> iterator) {
    Long address = createAttachedChain(readKeyBuffer, hash, iterator.next().getPayload());
    if (address == null) {
      return null;
    }

    if (iterator.hasNext()) {
      try (AttachedInternalChain chain = new AttachedInternalChain(address)) {
        do {
          if (!chain.append(iterator.next().getPayload())) {
            chain.free();
            return null;
          }
        } while (iterator.hasNext());
      }
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
          if (tail == from + OffHeapChainStorageEngine.this.totalChainHeaderSize) {
            tail = to + OffHeapChainStorageEngine.this.totalChainHeaderSize;
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

        long element = chain + OffHeapChainStorageEngine.this.totalChainHeaderSize;
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
        return keySize + OffHeapChainStorageEngine.this.totalChainHeaderSize + sizeOf(address + OffHeapChainStorageEngine.this.totalChainHeaderSize);
      } else {
        int elementSize = readElementLength(address);
        return ELEMENT_HEADER_SIZE + elementSize;
      }
    }
  }
}
