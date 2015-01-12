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

package org.ehcache.internal.store.disk;

import org.ehcache.Cache;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.Store;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.terracotta.statistics.observer.OperationObserver;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.terracotta.statistics.StatisticsBuilder.operation;

/**
 * Implements a persistent-to-disk store.
 * <p>
 * All new elements are automatically scheduled for writing to disk.
 *
 * @author Ludovic Orban
 */
public class DiskStore<K, V> implements Store<K, V> {

    private final DiskStorageFactory<K, V> diskStorageFactory;

    OperationObserver<CacheOperationOutcomes.EvictionOutcome> evictionOutcomeOperationObserver = operation(CacheOperationOutcomes.EvictionOutcome.class).named("eviction").of(this).build();;

    private final Segment<K, V>[] segments;
    private final int segmentShift;

    private volatile Set<? extends K> keySet;


    public DiskStore(final Configuration<K, V> config, String alias, TimeSource timeSource, boolean storeByValue) {
        diskStorageFactory = new DiskStorageFactory<K, V>(config.getClassLoader(), new DiskStorePathManager(), alias, true, 16, 16, 0, 30000, false);
        diskStorageFactory.bind(this);

        segments = new Segment[16];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment<K, V>(16, .75f, diskStorageFactory, evictionOutcomeOperationObserver);
        }

        this.segmentShift = Integer.numberOfLeadingZeros(segments.length - 1);
    }

    private static int hash(int hash) {
        int spread = hash;
        spread += (spread << 15 ^ 0xFFFFCD7D);
        spread ^= spread >>> 10;
        spread += (spread << 3);
        spread ^= spread >>> 6;
        spread += (spread << 2) + (spread << 14);
        return (spread ^ spread >>> 16);
    }

    private Segment<K, V> segmentFor(int hash) {
        return segments[hash >>> segmentShift];
    }

    @Override
    public ValueHolder<V> get(K key) throws CacheAccessException {
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> element = segmentFor(hash).get(key, hash, false);
        return element == null ? null : element.getValue();
    }

    @Override
    public boolean containsKey(K key) throws CacheAccessException {
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key, hash);
    }

    @Override
    public void put(K key, V value) throws CacheAccessException {
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> element = new DiskStorageFactory.ElementImpl<K, V>(key, value);
        DiskStorageFactory.Element<K, V> oldElement = segmentFor(hash).put(key, hash, element, false, false);
    }

    @Override
    public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
        return null;
    }

    @Override
    public void remove(K key) throws CacheAccessException {

    }

    @Override
    public boolean remove(K key, V value) throws CacheAccessException {
        return false;
    }

    @Override
    public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
        return false;
    }

    @Override
    public void clear() throws CacheAccessException {

    }

    @Override
    public void destroy() throws CacheAccessException {

    }

    @Override
    public void create() throws CacheAccessException {

    }

    @Override
    public void close() {
        diskStorageFactory.unbind();
    }

    @Override
    public void init() {

    }

    @Override
    public void maintenance() {

    }

    @Override
    public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {

    }

    @Override
    public void disableStoreEventNotifications() {

    }

    @Override
    public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
        return null;
    }

    @Override
    public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
        return null;
    }

    @Override
    public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        return null;
    }

    @Override
    public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
        return null;
    }

    @Override
    public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
        return null;
    }

    @Override
    public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        return null;
    }

    @Override
    public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
        return null;
    }

    @Override
    public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        return null;
    }

    @Override
    public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
        return null;
    }

    public boolean fault(K key, DiskStorageFactory.Placeholder placeholder, DiskStorageFactory.DiskMarker marker) {
        return false;
    }

    public void evict(K key, DiskStorageFactory.DiskSubstitute diskSubstitute) {

    }

    public DiskStorageFactory.DiskSubstitute<K, V> unretrievedGet(K key) {
        if (key == null) {
            return null;
        }

        int hash = hash(key.hashCode());
        DiskStorageFactory.DiskSubstitute<K, V> o = segmentFor(hash).unretrievedGet(key, hash);
        return o;
    }

    public Set<? extends K> keySet() {
        if (keySet != null) {
            return keySet;
        } else {
            keySet = new KeySet();
            return keySet;
        }
    }

    public void removeAll() {

    }

    public boolean putRawIfAbsent(K key, DiskStorageFactory.DiskMarker<K, V> marker) {
        return false;
    }

    public List<DiskStorageFactory.DiskSubstitute<K, V>> getRandomSample(ElementSubstituteFilter onDiskFilter, int min, K keyHint) {
        return null;
    }

    public DiskStorageFactory.Element evictElement(K key, DiskStorageFactory.DiskSubstitute target) {
        return null;
    }


    private int getSize() {
        int size = 0;
        for (Segment<K, V> segment : segments) {
            size += segment.count;
        }
        return size;
    }

    final class KeySet extends AbstractSet<K> {

        /**
         * {@inheritDoc}
         */
        @Override
        public java.util.Iterator<K> iterator() {
            return new KeyIterator();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int size() {
            return DiskStore.this.getSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean contains(Object o) {
            try {
                return DiskStore.this.containsKey((K) o);
            } catch (CacheAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean remove(Object o) {
            try {
                DiskStore.this.remove((K) o);
                return false;
            } catch (CacheAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void clear() {
            DiskStore.this.removeAll();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object[] toArray() {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this) {
                c.add(object);
            }
            return c.toArray();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T[] toArray(T[] a) {
            Collection<Object> c = new ArrayList<Object>();
            for (Object object : this) {
                c.add(object);
            }
            return c.toArray(a);
        }
    }

    abstract class HashIterator {
        private int segmentIndex;
        private java.util.Iterator<HashEntry> currentIterator;

        /**
         * Constructs a new HashIterator
         */
        HashIterator() {
            segmentIndex = segments.length;

            while (segmentIndex > 0) {
                segmentIndex--;
                currentIterator = segments[segmentIndex].hashIterator();
                if (currentIterator.hasNext()) {
                    return;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        public boolean hasNext() {
            if (this.currentIterator == null) {
                return false;
            }

            if (this.currentIterator.hasNext()) {
                return true;
            } else {
                while (segmentIndex > 0) {
                    segmentIndex--;
                    currentIterator = segments[segmentIndex].hashIterator();
                    if (currentIterator.hasNext()) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Returns the next hash-entry - called by subclasses
         *
         * @return next HashEntry
         */
        protected HashEntry<K, V> nextEntry() {
            if (currentIterator == null) {
                return null;
            }

            if (currentIterator.hasNext()) {
                return currentIterator.next();
            } else {
                while (segmentIndex > 0) {
                    segmentIndex--;
                    currentIterator = segments[segmentIndex].hashIterator();
                    if (currentIterator.hasNext()) {
                        return currentIterator.next();
                    }
                }
            }
            return null;
        }

        /**
         * {@inheritDoc}
         */
        public void remove() {
            currentIterator.remove();
        }

    }

    private final class KeyIterator extends HashIterator implements java.util.Iterator<K> {
        /**
         * {@inheritDoc}
         */
        public K next() {
            return super.nextEntry().key;
        }
    }

}
