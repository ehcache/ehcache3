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

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implements a persistent-to-disk store.
 * <p>
 * All new elements are automatically scheduled for writing to disk.
 *
 * @author Ludovic Orban
 */
public class DiskStore<K, V> implements Store<K, V> {

    private final Class<K> keyType;
    private final Class<V> valueType;
    private final TimeSource timeSource;
    private final Set<? extends K> keySet = new KeySet();
    private final String alias;
    private final ClassLoader classLoader;

    private volatile DiskStorageFactory<K, V> diskStorageFactory;
    private volatile Segment<K, V>[] segments;
    private volatile int segmentShift;


    public DiskStore(final Configuration<K, V> config, String alias, TimeSource timeSource) {
        this.alias = alias;
        this.keyType = config.getKeyType();
        this.valueType = config.getValueType();
        this.timeSource = timeSource;
        this.classLoader = config.getClassLoader();
    }

    private void checkKey(K keyObject) {
        if (keyObject == null) {
            throw new NullPointerException();
        }
        if (!keyType.isAssignableFrom(keyObject.getClass())) {
            throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
        }
    }

    private void checkValue(V valueObject) {
        if (valueObject == null) {
            throw new NullPointerException();
        }
        if (!valueType.isAssignableFrom(valueObject.getClass())) {
            throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
        }
    }

    private int size() {
        int size = 0;
        for (Segment<K, V> segment : segments) {
            size += segment.count;
        }
        return size;
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
        checkKey(key);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> existingElement = segmentFor(hash).get(key, hash, false);
        return existingElement == null ? null : existingElement.getValue();
    }

    @Override
    public boolean containsKey(K key) throws CacheAccessException {
        checkKey(key);
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key, hash);
    }

    @Override
    public void put(K key, V value) throws CacheAccessException {
        checkKey(key);
        checkValue(value);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> newElement = new DiskStorageFactory.ElementImpl<K, V>(key, value);
        segmentFor(hash).put(key, hash, newElement, false, false);
    }

    @Override
    public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
        checkKey(key);
        checkValue(value);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> newElement = new DiskStorageFactory.ElementImpl<K, V>(key, value);
        DiskStorageFactory.Element<K, V> existingElement = segmentFor(hash).put(key, hash, newElement, true, false);
        return existingElement == null ? null : existingElement.getValue();
    }

    @Override
    public void remove(K key) throws CacheAccessException {
        checkKey(key);
        int hash = hash(key.hashCode());
        segmentFor(hash).remove(key, hash, null);
    }

    @Override
    public boolean remove(K key, V value) throws CacheAccessException {
        checkKey(key);
        checkValue(value);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> newElement = new DiskStorageFactory.ElementImpl<K, V>(key, value);
        DiskStorageFactory.Element<K, V> existingElement = segmentFor(hash).remove(key, hash, newElement);
        return existingElement != null && existingElement.getValue() != null;
    }

    @Override
    public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
        checkKey(key);
        checkValue(value);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> newElement = new DiskStorageFactory.ElementImpl<K, V>(key, value);
        DiskStorageFactory.Element<K, V> existingElement = segmentFor(hash).replace(key, hash, newElement);
        return existingElement == null ? null : existingElement.getValue();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
        checkKey(key);
        checkValue(oldValue);
        checkValue(newValue);
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> oldElement = new DiskStorageFactory.ElementImpl<K, V>(key, oldValue);
        DiskStorageFactory.Element<K, V> newElement = new DiskStorageFactory.ElementImpl<K, V>(key, newValue);
        return segmentFor(hash).replace(key, hash, oldElement, newElement);
    }

    @Override
    public void clear() throws CacheAccessException {
        for (Segment s : segments) {
            s.clear();
        }
    }

    @Override
    public void destroy() throws CacheAccessException {
        diskStorageFactory.unbind(true);
        diskStorageFactory = null;
        segments = null;
    }

    @Override
    public void create() throws CacheAccessException {

    }

    @Override
    public void close() {
        diskStorageFactory.unbind(false);
        diskStorageFactory = null;
        segments = null;
    }

    @Override
    public void init() {
        diskStorageFactory = new DiskStorageFactory<K, V>(classLoader, timeSource, new DiskStorePathManager(), alias, true, 16, 16, 0, 30000, false);

        segments = new Segment[16];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment<K, V>(16, .75f, diskStorageFactory, timeSource);
        }

        this.segmentShift = Integer.numberOfLeadingZeros(segments.length - 1);

        diskStorageFactory.bind(this);
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
        final KeyIterator keyIterator = new KeyIterator();
        return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
            @Override
            public boolean hasNext() throws CacheAccessException {
                return keyIterator.hasNext();
            }

            @Override
            public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
                final K key = keyIterator.next();
                final ValueHolder<V> valueHolder = get(key);
                return new Cache.Entry<K, ValueHolder<V>>() {
                    @Override
                    public K getKey() {
                        return key;
                    }

                    @Override
                    public ValueHolder<V> getValue() {
                        return valueHolder;
                    }

                    @Override
                    public long getCreationTime(TimeUnit unit) {
                        return valueHolder == null ? 0 : valueHolder.creationTime(unit);
                    }

                    @Override
                    public long getLastAccessTime(TimeUnit unit) {
                        return valueHolder == null ? 0 : valueHolder.lastAccessTime(unit);
                    }

                    @Override
                    public float getHitRate(TimeUnit unit) {
                        return valueHolder == null ? 0 : valueHolder.hitRate(unit);
                    }
                };
            }
        };
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


    boolean fault(K key, DiskStorageFactory.Placeholder<K, V> expect, DiskStorageFactory.DiskMarker<K, V> fault) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).fault(key, hash, expect, fault, false);
    }

    void evict(K key, DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
        throw new UnsupportedOperationException();
    }

    DiskStorageFactory.DiskSubstitute<K, V> unretrievedGet(K key) {
        if (key == null) {
            return null;
        }

        int hash = hash(key.hashCode());
        DiskStorageFactory.DiskSubstitute<K, V> o = segmentFor(hash).unretrievedGet(key, hash);
        return o;
    }

    Set<? extends K> keySet() {
        return keySet;
    }

    boolean putRawIfAbsent(K key, DiskStorageFactory.DiskMarker<K, V> encoded) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).putRawIfAbsent(key, hash, encoded);
    }

    List<DiskStorageFactory.DiskSubstitute<K, V>> getRandomSample(ElementSubstituteFilter onDiskFilter, int min, K keyHint) {
        throw new UnsupportedOperationException();
    }

    DiskStorageFactory.Element<K, V> evictElement(K key, DiskStorageFactory.DiskSubstitute<K, V> target) {
        throw new UnsupportedOperationException();
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
            return DiskStore.this.size();
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
                //todo: fix return code
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
            try {
                DiskStore.this.clear();
            } catch (CacheAccessException e) {
                throw new RuntimeException(e);
            }
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
        private java.util.Iterator<HashEntry<K, V>> currentIterator;

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

    class KeyIterator extends HashIterator implements java.util.Iterator<K> {
        /**
         * {@inheritDoc}
         */
        public K next() {
            return super.nextEntry().key;
        }
    }

}
