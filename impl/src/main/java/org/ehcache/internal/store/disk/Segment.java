/**
 *  Copyright Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 *
 */
package org.ehcache.internal.store.disk;

import org.ehcache.function.BiFunction;
import org.ehcache.internal.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Segment implementation used in LocalStore.
 * <p>
 * The segment extends ReentrantReadWriteLock to allow read locking on read operations.
 * In addition to the typical CHM-like methods, this classes additionally supports
 * replacement under a read lock - which is accomplished using an atomic CAS on the
 * associated HashEntry.
 *
 * @author Chris Dennis
 * @author Ludovic Orban
 */
public class Segment<K, V> extends ReentrantReadWriteLock {

    private static final Logger LOG = LoggerFactory.getLogger(Segment.class.getName());

    private static final float LOAD_FACTOR = 0.75f;
    private static final int INITIAL_CAPACITY = 16;
    private static final int MAXIMUM_CAPACITY = Integer.highestOneBit(Integer.MAX_VALUE);

    /**
     * Count of elements in the map.
     * <p>
     * A volatile reference is needed here for the same reasons as in the table reference.
     */
    protected volatile int count;

    /**
     * Mod-count used to track concurrent modifications when doing size calculations or iterating over the store.
     * <p>
     * Note that we don't actually have any iterators yet...
     */
    protected int modCount;

    /**
     * The primary substitute factory.
     * <p>
     * This is the substitute type used to store <code>Element</code>s when they are first added to the store.
     */
    private final DiskStorageFactory<K, V> disk;

    /**
     * Table of HashEntry linked lists, indexed by the least-significant bits of the spread-hash value.
     * <p>
     * A volatile reference is needed to ensure the visibility of table changes made during rehash operations to size operations.
     * Key operations are done under read-locks so there is no need for volatility in that regard.  Hence if we switched to read-locked
     * size operations, we wouldn't need a volatile reference here.
     */
    private volatile HashEntry<K, V>[] table;

    /**
     * Size at which the next rehashing of this Segment should occur
     */
    private int threshold;

    private final TimeSource timeSource;

    /**
     * Create a Segment with the given initial capacity, load-factor, primary element substitute factory, and identity element substitute factory.
     * <p>
     * An identity element substitute factory is specified at construction time because only one subclass of IdentityElementProxyFactory
     * can be used with a Segment.  Without this requirement the mapping between bare {@link org.ehcache.internal.store.disk.DiskStorageFactory.Element} instances and the factory
     * responsible for them would be ambiguous.
     * <p>
     * If a <code>null</code> identity element substitute factory is specified then encountering a raw element (i.e. as a result of using an
     * identity element substitute factory) will result in a null pointer exception during decode.
     *
     * @param initialCapacity initial capacity of store
     * @param loadFactor fraction of capacity at which rehash occurs
     * @param primary primary element substitute factory
     */
    public Segment(int initialCapacity, float loadFactor, DiskStorageFactory<K, V> primary, TimeSource timeSource) {
        this.timeSource = timeSource;
        this.table = new HashEntry[initialCapacity];
        this.threshold = (int) (table.length * loadFactor);
        this.modCount = 0;
        this.disk = primary;
    }

    public Segment(DiskStorageFactory<K, V> primary, TimeSource timeSource) {
        this(INITIAL_CAPACITY, LOAD_FACTOR, primary, timeSource);
    }

    private HashEntry<K, V> getFirst(int hash) {
        HashEntry<K, V>[] tab = table;
        return tab[hash & (tab.length - 1)];
    }

    /**
     * Decode the possible DiskSubstitute
     *
     * @param substitute the DiskSubstitute to decode
     * @return the decoded DiskSubstitute
     */
    private DiskStorageFactory.Element<K, V> decode(DiskStorageFactory.DiskSubstitute<K, V> substitute) {
        return substitute.getFactory().retrieve(substitute);
    }

    /**
     * Decode the possible DiskSubstitute, updating the statistics
     *
     * @param substitute the DiskSubstitute to decode
     * @return the decoded DiskSubstitute
     */
    private DiskStorageFactory.Element<K, V> decodeHit(DiskStorageFactory.DiskSubstitute<K, V> substitute) {
        return substitute.getFactory().retrieve(substitute, this);
    }

    /**
     * Free the DiskSubstitute
     *
     * @param diskSubstitute the DiskSubstitute to free
     */
    private void free(DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
        free(diskSubstitute, false);
    }

    /**
     * Free the DiskSubstitute indicating if it could not be faulted
     *
     * @param diskSubstitute the DiskSubstitute to free
     * @param faultFailure true if the DiskSubstitute should be freed because of a fault failure
     */
    private void free(DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute, boolean faultFailure) {
        diskSubstitute.getFactory().free(writeLock(), diskSubstitute, faultFailure);
    }

    /**
     * Get the element mapped to this key (or null if there is no mapping for this key)
     *
     *
     * @param key key to lookup
     * @param hash spread-hash for this key
     * @param markFaulted
     * @return mapped element
     */
    DiskStorageFactory.Element<K, V> get(K key, int hash, final boolean markFaulted) {
        readLock().lock();
        try {
            // read-volatile
            if (count != 0) {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        if (markFaulted) {
                            e.faulted.set(true);
                        }
                        return decodeHit(e.element);
                    }
                    e = e.next;
                }
            }
            return null;
        } finally {
            readLock().unlock();
        }
    }

    /**
     * Return the unretrieved (undecoded) value for this key
     *
     * @param key key to lookup
     * @param hash spread-hash for the key
     * @return Element or ElementSubstitute
     */
    DiskStorageFactory.DiskSubstitute<K, V> unretrievedGet(K key, int hash) {
        readLock().lock();
        try {
            if (count != 0) {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return e.element;
                    }
                    e = e.next;
                }
            }
        } finally {
            readLock().unlock();
        }
        return null;
    }

    /**
     * Return true if this segment contains a mapping for this key
     *
     * @param key key to check for
     * @param hash spread-hash for key
     * @return <code>true</code> if there is a mapping for this key
     */
    boolean containsKey(K key, int hash) {
        readLock().lock();
        try {
            // read-volatile
            if (count != 0) {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return true;
                    }
                    e = e.next;
                }
            }
            return false;
        } finally {
            readLock().unlock();
        }
    }

    /**
     * Replace the element mapped to this key only if currently mapped to the given element.
     *
     *
     * @param key key to map the element to
     * @param hash spread-hash for the key
     * @param oldElement expected element
     * @param newElement element to add
     * @return <code>true</code> on a successful replace
     */
    boolean replace(K key, int hash, DiskStorageFactory.Element<K, V> oldElement, DiskStorageFactory.Element<K, V> newElement) {
        boolean installed = false;
        DiskStorageFactory.DiskSubstitute<K, V> encoded = disk.create(newElement);

        writeLock().lock();
        try {
            HashEntry<K, V> e = getFirst(hash);
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            boolean replaced = false;
            if (e != null && eq(decode(e.element), oldElement)) {
                replaced = true;
                /*
                 * make sure we re-get from the HashEntry - since the decode in the conditional
                 * may have faulted in a different type - we must make sure we know what type
                 * to do the increment/decrement on.
                 */
                DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;

                e.element = encoded;
                e.faulted.set(false);
                installed = true;
                free(onDiskSubstitute);

            } else {
                free(encoded);
            }
            return replaced;
        } finally {
            writeLock().unlock();

            if (installed) {
                encoded.installed();
            }
        }
    }

    /**
     * Replace the entry for this key only if currently mapped to some element.
     *
     * @param key key to map the element to
     * @param hash spread-hash for the key
     * @param newElement element to add
     * @return previous element mapped to this key
     */
    DiskStorageFactory.Element<K, V> replace(K key, int hash, DiskStorageFactory.Element<K, V> newElement) {
        boolean installed = false;
        DiskStorageFactory.DiskSubstitute<K, V> encoded = disk.create(newElement);

        writeLock().lock();
        try {
            HashEntry<K, V> e = getFirst(hash);
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            DiskStorageFactory.Element<K, V> oldElement = null;
            if (e != null) {
                DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;

                e.element = encoded;
                e.faulted.set(false);
                installed = true;
                oldElement = decode(onDiskSubstitute);
                free(onDiskSubstitute);
            } else {
                free(encoded);
            }

            return oldElement;
        } finally {
            writeLock().unlock();

            if (installed) {
                encoded.installed();
            }
        }
    }

    /**
     * Add the supplied mapping.
     * <p>
     * The supplied element is substituted using the primary element proxy factory
     * before being stored in the cache.  If <code>onlyIfAbsent</code> is set
     * then the mapping will only be added if no element is currently mapped
     * to that key.
     *
     * @param key key to map the element to
     * @param hash spread-hash for the key
     * @param element element to store
     * @param onlyIfAbsent if true does not replace existing mappings
     * @return previous element mapped to this key
     */
    DiskStorageFactory.Element<K, V> put(K key, int hash, DiskStorageFactory.Element<K, V> element, boolean onlyIfAbsent, boolean faulted) {
        boolean installed = false;
        DiskStorageFactory.DiskSubstitute<K, V> encoded = disk.create(element);

        writeLock().lock();
        try {
            // ensure capacity
            if (count + 1 > threshold) {
                rehash();
            }
            HashEntry<K, V>[] tab = table;
            int index = hash & (tab.length - 1);
            HashEntry<K, V> first = tab[index];
            HashEntry<K, V> e = first;
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            DiskStorageFactory.Element<K, V> oldElement;
            if (e != null) {
                DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;
                if (!onlyIfAbsent) {
                    e.element = encoded;
                    installed = true;
                    oldElement = decode(onDiskSubstitute);

                    free(onDiskSubstitute);
                    e.faulted.set(faulted);
                } else {
                    oldElement = decode(onDiskSubstitute);

                    free(encoded);
                }
            } else {
                oldElement = null;
                ++modCount;
                tab[index] = new HashEntry<K, V>(key, hash, first, encoded, new AtomicBoolean(faulted));
                installed = true;
                // write-volatile
                count = count + 1;
            }
            return oldElement;

        } finally {
            writeLock().unlock();

            if (installed) {
                encoded.installed();
            }
        }
    }


    /**
     * Add the supplied pre-encoded mapping.
     * <p>
     * The supplied encoded element is directly inserted into the segment
     * if there is no other mapping for this key.
     *
     * @param key key to map the element to
     * @param hash spread-hash for the key
     * @param encoded encoded element to store
     * @return <code>true</code> if the encoded element was installed
     * @throws IllegalArgumentException if the supplied key is already present
     */
    boolean putRawIfAbsent(K key, int hash, DiskStorageFactory.DiskMarker<K, V> encoded) throws IllegalArgumentException {
        writeLock().lock();
        try {
            // ensure capacity
            if (count + 1 > threshold) {
                rehash();
            }
            HashEntry<K, V>[] tab = table;
            int index = hash & (tab.length - 1);
            HashEntry<K, V> first = tab[index];
            HashEntry<K, V> e = first;
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            if (e == null) {
                ++modCount;
                tab[index] = new HashEntry<K, V>(key, hash, first, encoded, new AtomicBoolean(false));
                // write-volatile
                count = count + 1;
                return true;
            } else {
                throw new IllegalArgumentException("Duplicate key detected");
            }
        } finally {
            writeLock().unlock();
        }
    }

    private void rehash() {
        HashEntry<K, V>[] oldTable = table;
        int oldCapacity = oldTable.length;
        if (oldCapacity >= MAXIMUM_CAPACITY) {
            return;
        }

        /*
         * Reclassify nodes in each list to new Map.  Because we are
         * using power-of-two expansion, the elements from each bin
         * must either stay at same index, or move with a power of two
         * offset. We eliminate unnecessary node creation by catching
         * cases where old nodes can be reused because their next
         * fields won't change. Statistically, at the default
         * threshold, only about one-sixth of them need cloning when
         * a table doubles. The nodes they replace will be garbage
         * collectable as soon as they are no longer referenced by any
         * reader thread that may be in the midst of traversing table
         * right now.
         */

        HashEntry<K, V>[] newTable = new HashEntry[oldCapacity << 1];
        threshold = (int)(newTable.length * LOAD_FACTOR);
        int sizeMask = newTable.length - 1;
        for (int i = 0; i < oldCapacity; i++) {
            // We need to guarantee that any existing reads of old Map can
            //  proceed. So we cannot yet null out each bin.
            HashEntry<K, V> e = oldTable[i];

            if (e != null) {
                HashEntry<K, V> next = e.next;
                int idx = e.hash & sizeMask;

                //  Single node on list
                if (next == null) {
                    newTable[idx] = e;
                } else {
                    // Reuse trailing consecutive sequence at same slot
                    HashEntry<K, V> lastRun = e;
                    int lastIdx = idx;
                    for (HashEntry<K, V> last = next;
                         last != null;
                         last = last.next) {
                        int k = last.hash & sizeMask;
                        if (k != lastIdx) {
                            lastIdx = k;
                            lastRun = last;
                        }
                    }
                    newTable[lastIdx] = lastRun;

                    // Clone all remaining nodes
                    for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
                        int k = p.hash & sizeMask;
                        HashEntry<K, V> n = newTable[k];
                        newTable[k] = new HashEntry<K, V>(p.key, p.hash, n, p.element, p.faulted);
                    }
                }
            }
        }
        table = newTable;
    }

    /**
     * Remove the matching mapping.
     * <p>
     * If <code>value</code> is <code>null</code> then match on the key only,
     * else match on both the key and the value.
     *
     *
     * @param key key to match against
     * @param hash spread-hash for the key
     * @param value optional value to match against
     * @return removed element
     */
    DiskStorageFactory.Element<K, V> remove(K key, int hash, DiskStorageFactory.Element<K, V> value) {
        writeLock().lock();
        try {
            HashEntry<K, V>[] tab = table;
            int index = hash & (tab.length - 1);
            HashEntry<K, V> first = tab[index];
            HashEntry<K, V> e = first;
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            DiskStorageFactory.Element<K, V> oldValue = null;
            if (e != null) {
                oldValue = decode(e.element);
                if (value == null || eq(value, oldValue)) {
                    // All entries following removed node can stay
                    // in list, but all preceding ones need to be
                    // cloned.
                    ++modCount;
                    HashEntry<K, V> newFirst = e.next;
                    for (HashEntry<K, V> p = first; p != e; p = p.next) {
                        newFirst = new HashEntry<K, V>(p.key, p.hash, newFirst, p.element, p.faulted);
                    }
                    tab[index] = newFirst;
                    /*
                     * make sure we re-get from the HashEntry - since the decode in the conditional
                     * may have faulted in a different type - we must make sure we know what type
                     * to do the free on.
                     */
                    DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;
                    free(onDiskSubstitute);

                    // write-volatile
                    count = count - 1;
                } else {
                    oldValue = null;
                }
            }

            if (oldValue == null) {
                LOG.debug("remove deleted nothing");
            }

            return oldValue;
        } finally {
            writeLock().unlock();
        }
    }

    /**
     * Removes all mappings from this segment.
     */
    void clear() {
        writeLock().lock();
        try {
            if (count != 0) {
                HashEntry<K, V>[] tab = table;
                for (int i = 0; i < tab.length; i++) {
                    for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
                        free(e.element);
                    }
                    tab[i] = null;
                }
                ++modCount;
                // write-volatile
                count = 0;
            }
        } finally {
            writeLock().unlock();
        }
    }

    /**
     * Try to atomically switch (CAS) the <code>expect</code> representation of this element for the
     * <code>fault</code> representation.
     * <p>
     * A successful switch will return <code>true</code>, and free the replaced element/element-proxy.
     * A failed switch will return <code>false</code> and free the element/element-proxy which was not
     * installed.  Unlike <code>fault</code> this method can return <code>false</code> if the object
     * could not be installed due to lock contention.
     *
     * @param key key to which this element (proxy) is mapped
     * @param hash the hash of the key
     * @param expect element (proxy) expected
     * @param fault element (proxy) to install
     * @return <code>true</code> if <code>fault</code> was installed
     */
    boolean fault(K key, int hash, DiskStorageFactory.Placeholder<K, V> expect, DiskStorageFactory.DiskMarker<K, V> fault, final boolean skipFaulted) {
        writeLock().lock();
        try {
            return faultInternal(key, hash, expect, fault, skipFaulted);
        } finally {
            writeLock().unlock();
        }
    }

    // TODO Needs some serious clean up !
    private boolean faultInternal(final K key, final int hash, final DiskStorageFactory.Placeholder<K, V> expect, final DiskStorageFactory.DiskMarker<K, V> fault, final boolean skipFaulted) {
        boolean faulted = false;
        if (count != 0) {
            HashEntry<K, V> e = getFirst(hash);
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    faulted = e.faulted.get();
                }
                e = e.next;
            }

            if (skipFaulted && faulted) {
                free(fault, false);
                return true;
            }

            if (findAndFree(key, hash, expect, fault)) {
                return true;
            }
        }
        free(fault, true);
        return false;
    }

    private boolean findAndFree(final K key, final int hash, final DiskStorageFactory.Placeholder<K, V> expect, final DiskStorageFactory.DiskMarker<K, V> fault) {
        for (HashEntry<K, V> e = getFirst(hash); e != null; e = e.next) {
            if (e.hash == hash && key.equals(e.key)) {
                if (expect == e.element) {
                    e.element = fault;
                    free(expect);
                    return true;
                }
            }
        }
        return false;
    }

    @Deprecated
    private boolean returnSafeDeprecated(final K key, final int hash, final DiskStorageFactory.Element<K, V> element) {
        notifyEviction(remove(key, hash, null));
        return false;
    }

    private void notifyEviction(final DiskStorageFactory.Element<K, V> evicted) {
    }

    /**
     * Remove the matching mapping.  Unlike the {@link #remove(Object, int, org.ehcache.internal.store.disk.DiskStorageFactory.Element)} (Object, int, net.sf.ehcache.Element, net.sf.ehcache.store.ElementValueComparator)} method
     * evict does referential comparison of the unretrieved substitute against the argument value.
     *
     * @param key key to match against
     * @param hash spread-hash for the key
     * @param value optional value to match against
     * @return <code>true</code> on a successful remove
     */
    DiskStorageFactory.Element<K, V> evict(K key, int hash, DiskStorageFactory.DiskSubstitute<K, V> value) {
        return evict(key, hash, value, true);
    }

    /**
     * Remove the matching mapping.  Unlike the {@link #remove(Object, int, org.ehcache.internal.store.disk.DiskStorageFactory.Element)} (Object, int, net.sf.ehcache.Element, net.sf.ehcache.store.ElementValueComparator)} method
     * evict does referential comparison of the unretrieved substitute against the argument value.
     *
     * @param key key to match against
     * @param hash spread-hash for the key
     * @param value optional value to match against
     * @param notify whether to notify if we evict something
     * @return <code>true</code> on a successful remove
     */
    DiskStorageFactory.Element<K, V> evict(K key, int hash, DiskStorageFactory.DiskSubstitute<K, V> value, boolean notify) {
        if (writeLock().tryLock()) {
            DiskStorageFactory.Element<K, V> evictedElement = null;
            try {
                HashEntry<K, V>[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }

                if (e != null && !e.faulted.get()) {
                    evictedElement = decode(e.element);
                }

                // TODO this has to be removed!
                if (e != null && (value == null || value == e.element) && !e.faulted.get()) {
                    // All entries following removed node can stay
                    // in list, but all preceding ones need to be
                    // cloned.
                    ++modCount;
                    HashEntry<K, V> newFirst = e.next;
                    for (HashEntry<K, V> p = first; p != e; p = p.next) {
                        newFirst = new HashEntry<K, V>(p.key, p.hash, newFirst, p.element, p.faulted);
                    }
                    tab[index] = newFirst;
                    /*
                     * make sure we re-get from the HashEntry - since the decode in the conditional
                     * may have faulted in a different type - we must make sure we know what type
                     * to do the free on.
                     */
                    DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;
                    free(onDiskSubstitute);

                    // write-volatile
                    count = count - 1;
                } else {
                    evictedElement = null;
                }
                return evictedElement;
            } finally {
                writeLock().unlock();
                if (notify && evictedElement != null) {
                    if (evictedElement.isExpired(timeSource.getTimeMillis())) {
                        // todo: stats
                    } else {
                        // todo: stats
                    }
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Select a random sample of elements generated by the supplied factory.
     *
     * @param filter filter of substitute types
     * @param sampleSize minimum number of elements to return
     * @param sampled collection in which to place the elements
     * @param seed random seed for the selection
     */
    void addRandomSample(ElementSubstituteFilter filter, int sampleSize, Collection<DiskStorageFactory.DiskSubstitute<K, V>> sampled, int seed) {
        if (count == 0) {
            return;
        }
        final HashEntry[] tab = table;
        final int tableStart = seed & (tab.length - 1);
        int tableIndex = tableStart;
        do {
            for (HashEntry<K, V> e = tab[tableIndex]; e != null; e = e.next) {
                Object value = e.element;
                if (!e.faulted.get() && filter.allows(value)) {
                    sampled.add((DiskStorageFactory.DiskSubstitute) value);
                }
            }

            if (sampled.size() >= sampleSize) {
                return;
            }

            //move to next table slot
            tableIndex = (tableIndex + 1) & (tab.length - 1);
        } while (tableIndex != tableStart);
    }

    /**
     * Creates an iterator over the HashEntry objects within this Segment.
     * @return an iterator over the HashEntry objects within this Segment.
     */
    Iterator<HashEntry<K, V>> hashIterator() {
        return new HashIterator();
    }

    @Override
    public String toString() {
        return super.toString() + " count: " + count;
    }

    /**
     * Will check whether a Placeholder that failed to flush to disk is lying around
     * If so, it'll try to evict it
     * @param key the key
     * @param hash the key's hash
     * @return true if a failed marker was or is still there, false otherwise
     */
    boolean cleanUpFailedMarker(final K key, final int hash) {
        boolean readLocked = false;
        boolean failedMarker = false;
        if (!isWriteLockedByCurrentThread()) {
            readLock().lock();
            readLocked = true;
        }
        DiskStorageFactory.DiskSubstitute<K, V> substitute = null;
        try {
            if (count != 0) {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        substitute = e.element;
                        if (substitute instanceof DiskStorageFactory.Placeholder) {
                            failedMarker = ((DiskStorageFactory.Placeholder)substitute).hasFailedToFlush();
                            break;
                        }
                    }
                    e = e.next;
                }
            }
        } finally {
            if (readLocked) {
                readLock().unlock();
            }
        }
        if (failedMarker) {
            evict(key, hash, substitute, false);
        }
        return failedMarker;
    }

    /**
     * Marks an entry as flushable to disk (i.e. not faulted in higher tiers)
     * Also updates the access stats
     * @param key the key
     * @param hash they hash
     * @param element the expected element
     * @return true if succeeded
     */
    boolean flush(final K key, final int hash, final DiskStorageFactory.Element<K, V> element) {
        DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute = null;
        readLock().lock();
        try {
            HashEntry<K, V> e = getFirst(hash);
            while (e != null) {
                if (e.hash == hash && key.equals(e.key)) {
                    final boolean b = e.faulted.compareAndSet(true, false);
                    diskSubstitute = e.element;
                    if (diskSubstitute instanceof DiskStorageFactory.Placeholder) {
                        if (((DiskStorageFactory.Placeholder)diskSubstitute).hasFailedToFlush() && evict(key, hash, diskSubstitute) != null) {
                            diskSubstitute = null;
                        }
                    } else {
                        if (diskSubstitute instanceof DiskStorageFactory.DiskMarker) {
                            final DiskStorageFactory.DiskMarker<K, V> diskMarker = (DiskStorageFactory.DiskMarker)diskSubstitute;
                            diskMarker.updateStats(element);
                        }
                    }
                    return b;
                }
                e = e.next;
            }
        } finally {
            readLock().unlock();
            if (diskSubstitute != null && element.isExpired(timeSource.getTimeMillis())) {
                evict(key, hash, diskSubstitute);
            }
        }
        return false;
    }

    /**
     * Clears the faulted but on all entries
     */
    void clearFaultedBit() {
        writeLock().lock();
        try {
            HashEntry<K, V> entry;
            for (HashEntry<K, V> hashEntry : table) {
                entry = hashEntry;
                while (entry != null) {
                    entry.faulted.set(false);
                    entry = entry.next;
                }
            }
        } finally {
            writeLock().unlock();
        }
    }

    /**
     * Verifies if the mapping for a key is marked as faulted
     * @param key the key to check the mapping for
     * @return true if faulted, false otherwise (including no mapping)
     */
    public boolean isFaulted(final int hash, final K key) {
        readLock().lock();
        try {
            // read-volatile
            if (count != 0) {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return e.faulted.get();
                    }
                    e = e.next;
                }
            }
            return false;
        } finally {
            readLock().unlock();
        }
    }

    public DiskStorageFactory.Element<K, V> compute(K key, int hash, BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>> mappingFunction, Compute compute) {
        boolean installed = false;
        DiskStorageFactory.DiskSubstitute<K, V> encoded = null;

        writeLock().lock();
        try {
            // ensure capacity
            if (count + 1 > threshold) {
                rehash();
            }
            HashEntry<K, V>[] tab = table;
            int index = hash & (tab.length - 1);
            HashEntry<K, V> first = tab[index];
            HashEntry<K, V> e = first;
            while (e != null && (e.hash != hash || !key.equals(e.key))) {
                e = e.next;
            }

            DiskStorageFactory.Element<K, V> newElement;
            if (e != null) {
                if (compute == Compute.IF_ABSENT) {
                    newElement = null;
                } else {
                    DiskStorageFactory.DiskSubstitute<K, V> onDiskSubstitute = e.element;
                    installed = true;
                    DiskStorageFactory.Element<K, V> oldElement = decode(onDiskSubstitute);

                    newElement = mappingFunction.apply(key, oldElement);
                    if (newElement == null) {
                        remove(key, hash, null);
                    } else {
                        encoded = disk.create(newElement);

                        e.element = encoded;

                        free(onDiskSubstitute);
                        e.faulted.set(false);
                    }
                }
            } else {
                if (compute == Compute.IF_PRESENT) {
                    newElement = null;
                } else {
                    newElement = mappingFunction.apply(key, null);
                    if (newElement == null) {
                        remove(key, hash, null);
                    } else {
                        encoded = disk.create(newElement);

                        ++modCount;
                        tab[index] = new HashEntry<K, V>(key, hash, first, encoded, new AtomicBoolean(false));
                        installed = true;
                        // write-volatile
                        count = count + 1;
                    }
                }
            }

            return newElement;
        } finally {
            writeLock().unlock();

            if (installed && encoded != null) {
                encoded.installed();
            }
        }
    }

    private boolean eq(DiskStorageFactory.Element<K, V> e1, DiskStorageFactory.Element<K, V> e2) {
        V v1 = e1.getValueHolder().value();
        V v2 = e2.getValueHolder().value();

        if (v1 == v2) return true;
        if (v1 == null || v2 == null) return false;
        return v1.equals(v2);
    }


    static enum Compute {
        ALWAYS,
        IF_ABSENT,
        IF_PRESENT
    }

    /**
     * An iterator over the HashEntry objects within this Segment.
     */
    final class HashIterator implements Iterator<HashEntry<K, V>> {
        private int nextTableIndex;
        private final HashEntry<K, V>[] ourTable;
        private HashEntry<K, V> nextEntry;
        private HashEntry<K, V> lastReturned;

        private HashIterator() {
            if (count != 0) {
                ourTable = table;
                for (int j = ourTable.length - 1; j >= 0; --j) {
                    nextEntry = ourTable[j];
                    if (nextEntry != null) {
                        nextTableIndex = j - 1;
                        return;
                    }
                }
            } else {
                ourTable = null;
                nextTableIndex = -1;
            }
            advance();
        }

        private void advance() {
            if (nextEntry != null) {
                nextEntry = nextEntry.next;
                if (nextEntry != null) {
                    return;
                }
            }

            while (nextTableIndex >= 0) {
                nextEntry = ourTable[nextTableIndex--];
                if (nextEntry != null) {
                    return;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        public boolean hasNext() {
            return nextEntry != null;
        }

        /**
         * {@inheritDoc}
         */
        public HashEntry<K, V> next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        /**
         * {@inheritDoc}
         */
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            Segment.this.remove(lastReturned.key, lastReturned.hash, null);
            lastReturned = null;
        }
    }
}
