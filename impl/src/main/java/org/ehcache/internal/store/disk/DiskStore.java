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
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * Implements a persistent-to-disk store.
 * <p>
 * All new elements are automatically scheduled for writing to disk.
 *
 * @author Ludovic Orban
 */
public class DiskStore<K, V> implements Store<K, V> {

    static final long NO_EXPIRE = -1;

    private final Class<K> keyType;
    private final Class<V> valueType;
    private final TimeSource timeSource;
    private final Set<? extends K> keySet = new KeySet();
    private final String alias;
    private final ClassLoader classLoader;
    private final Expiry<? super K, ? super V> expiry;

    private volatile DiskStorageFactory<K, V> diskStorageFactory;
    private volatile Segment<K, V>[] segments;
    private volatile int segmentShift;


    public DiskStore(final Configuration<K, V> config, String alias, TimeSource timeSource) {
        this.alias = alias;
        this.keyType = config.getKeyType();
        this.valueType = config.getValueType();
        this.timeSource = timeSource;
        this.classLoader = config.getClassLoader();
        this.expiry = config.getExpiry();
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

    private Segment<K, V> segmentFor(int hash) throws CacheAccessException {
        if (segments == null) {
            throw new CacheAccessException("disk store is closed");
        }
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
        if (segments != null) {
            for (Segment s : segments) {
                s.clear();
            }
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

    private static final NullaryFunction<Boolean> REPLACE_EQUALS_TRUE = new NullaryFunction<Boolean>() {
        @Override
        public Boolean apply() {
            return Boolean.TRUE;
        }
    };

    private static boolean eq(Object o1, Object o2) {
        return (o1 == o2) || (o1 != null && o1.equals(o2));
    }

    private void setAccessTimeAndExpiry(K key, DiskStorageFactory.Element<K, V> element, long now) {
        element.getValue().setAccessTimeMillis(now);

        DiskValueHolder<V> valueHolder = element.getValue();
        Duration duration = expiry.getExpiryForAccess(key, valueHolder.value());
        if (duration != null) {
            if (duration.isForever()) {
                valueHolder.setExpireTimeMillis(NO_EXPIRE);
            } else {
                valueHolder.setExpireTimeMillis(safeExpireTime(now, duration));
            }
        }
    }

    private static long safeExpireTime(long now, Duration duration) {
        long millis = TimeUnit.MILLISECONDS.convert(duration.getAmount(), duration.getTimeUnit());

        if (millis == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        long result = now + millis;
        if (result < 0) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private DiskStorageFactory.Element<K, V> newUpdateValueHolder(K key, DiskStorageFactory.Element<K, V> oldValue, V newValue, long now) {
        if (oldValue == null || newValue == null) {
            throw new NullPointerException();
        }

        Duration duration = expiry.getExpiryForUpdate(key, oldValue.getValue().value(), newValue);
        if (Duration.ZERO.equals(duration)) {
            return null;
        }

        return new DiskStorageFactory.ElementImpl<K, V>(key, newValue);
    }

    private DiskStorageFactory.Element<K, V> newCreateValueHolder(K key, V value, long now) {
        if (value == null) {
            throw new NullPointerException();
        }

        Duration duration = expiry.getExpiryForCreation(key, value);
        if (Duration.ZERO.equals(duration)) {
            return null;
        }

        return new DiskStorageFactory.ElementImpl<K, V>(key, value);
    }

    DiskValueHolder<V> enforceCapacityIfValueNotNull(final DiskStorageFactory.Element<K, V> computeResult) {
        return computeResult == null ? null : computeResult.getValue();
    }

/*
    ValueHolder<V> enforceCapacityIfValueNotNull(final DiskValueHolder<V> computeResult) {
        if (computeResult != null) {
            enforceCapacity(1);
        }
        return computeResult;
    }

    private void enforceCapacity(int delta) {
        for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO * delta && evicted < EVICTION_RATIO * delta
            && capacityConstraint.compareTo((long) size()) < 0; attempts++) {
            if (evict()) {
                evicted++;
            }
        }
    }

    boolean evict() {
        evictionObserver.begin();
        final Random random = new Random();
        final int sampleSize = 8;

        @SuppressWarnings("unchecked")
        Set<Map.Entry<K, OnHeapValueHolder<V>>> values = map.getRandomValues(random, sampleSize, (Predicate<Map.Entry<K, OnHeapValueHolder<V>>>)evictionVeto);

        if (values.isEmpty()) {
            // 2nd attempt without any veto
            values = map.getRandomValues(random, sampleSize, Predicates.<Map.Entry<K, OnHeapValueHolder<V>>>none());
        }

        if (values.isEmpty()) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            Map.Entry<K, OnHeapValueHolder<V>> evict = Collections.max(values, (Comparator<? super Map.Entry<K, OnHeapValueHolder<V>>>) evictionPrioritizer);

            if (map.remove(evict.getKey(), evict.getValue())) {
                evictionObserver.end(CacheOperationOutcomes.EvictionOutcome.SUCCESS);
                eventListener.onEviction(wrap(evict));
                return true;
            } else {
                evictionObserver.end(CacheOperationOutcomes.EvictionOutcome.FAILURE);
                return false;
            }
        }
    }
*/

    @Override
    public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
        return compute(key, mappingFunction, REPLACE_EQUALS_TRUE);
    }

    @Override
    public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        checkKey(key);
        int hash = hash(key.hashCode());
        final long now = timeSource.getTimeMillis();

        BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>> biFunction = new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
            @Override
            public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedElement) {
                if (mappedElement != null && mappedElement.isExpired(now)) {
                    mappedElement = null;
                }

                V existingValue = mappedElement == null ? null : mappedElement.getValue().value();
                V computedValue = mappingFunction.apply(mappedKey, existingValue);
                if (computedValue == null) {
                    return null;
                } else if ((eq(existingValue, computedValue)) && (! replaceEqual.apply())) {
                    if (mappedElement != null) {
                        setAccessTimeAndExpiry(key, mappedElement, now);
                    }
                    return mappedElement;
                }

                checkValue(computedValue);
                if (mappedElement != null) {
                    return newUpdateValueHolder(key, mappedElement, computedValue, now);
                } else {
                    return newCreateValueHolder(key, computedValue, now);
                }
            }
        };

        DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.ALWAYS);
        return computedElement == null ? null : computedElement.getValue();
    }

    @Override
    public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
        checkKey(key);
        int hash = hash(key.hashCode());
        final long now = timeSource.getTimeMillis();

        BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>> biFunction = new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
            @Override
            public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedElement) {
                if (mappedElement == null || mappedElement.isExpired(now)) {
                    V computedValue = mappingFunction.apply(mappedKey);
                    if (computedValue == null) {
                        return null;
                    }

                    checkValue(computedValue);
                    return newCreateValueHolder(key, computedValue, now);
                } else {
                    setAccessTimeAndExpiry(key, mappedElement, now);
                    return mappedElement;
                }
            }
        };
        DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.IF_ABSENT);
        return enforceCapacityIfValueNotNull(computedElement);
    }

    @Override
    public ValueHolder<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
        return computeIfPresent(key, remappingFunction, REPLACE_EQUALS_TRUE);
    }

    @Override
    public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        checkKey(key);
        int hash = hash(key.hashCode());

        BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>> biFunction = new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
            @Override
            public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedElement) {
                final long now = timeSource.getTimeMillis();

                if (mappedElement != null && mappedElement.isExpired(now)) {
                    return null;
                }

                V existingValue = mappedElement == null ? null : mappedElement.getValue().value();

                V computedValue = remappingFunction.apply(mappedKey, existingValue);
                if (computedValue == null) {
                    return null;
                }

                if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
                    setAccessTimeAndExpiry(key, mappedElement, now);
                    return mappedElement;
                }

                checkValue(computedValue);
                return newUpdateValueHolder(key, mappedElement, computedValue, now);            }
        };

        DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.IF_PRESENT);
        return computedElement == null ? null : computedElement.getValue();
    }

    @Override
    public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
        return bulkCompute(keys, remappingFunction, REPLACE_EQUALS_TRUE);
    }

    @Override
    public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, final Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
        Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
        for (K key : keys) {
            checkKey(key);
            BiFunction<K, V, V> biFunction = new BiFunction<K, V, V>() {
                @Override
                public V apply(final K k, final V v) {
                    Map.Entry<K, V> entry = new Map.Entry<K, V>() {
                        @Override
                        public K getKey() {
                            return k;
                        }

                        @Override
                        public V getValue() {
                            return v;
                        }

                        @Override
                        public V setValue(V value) {
                            throw new UnsupportedOperationException();
                        }
                    };
                    java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = remappingFunction.apply(Collections.singleton(entry)).iterator();
                    Map.Entry<? extends K, ? extends V> result = iterator.next();
                    if (result != null) {
                        checkKey(result.getKey());
                        return result.getValue();
                    } else {
                        return null;
                    }
                }
            };
            ValueHolder<V> computed = compute(key, biFunction, replaceEqual);
            result.put(key, computed);
        }
        return result;
    }

    @Override
    public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, final Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
        Map<K, ValueHolder<V>> result = new HashMap<K, ValueHolder<V>>();
        for (K key : keys) {
            checkKey(key);
            Function<K, V> function = new Function<K, V>() {
                @Override
                public V apply(K k) {
                    java.util.Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = mappingFunction.apply(Collections.singleton(k)).iterator();
                    Map.Entry<? extends K, ? extends V> result = iterator.next();
                    if (result != null) {
                        checkKey(result.getKey());
                        return result.getValue();
                    } else {
                        return null;
                    }
                }
            };
            ValueHolder<V> computed = computeIfAbsent(key, function);
            result.put(key, computed);
        }
        return result;
    }


    boolean fault(K key, DiskStorageFactory.Placeholder<K, V> expect, DiskStorageFactory.DiskMarker<K, V> fault) {
        try {
            int hash = hash(key.hashCode());
            return segmentFor(hash).fault(key, hash, expect, fault, false);
        } catch (CacheAccessException e) {
            throw new RuntimeException(e);
        }
    }

    void evict(K key, DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
        throw new UnsupportedOperationException();
    }

    DiskStorageFactory.DiskSubstitute<K, V> unretrievedGet(K key) {
        if (key == null) {
            return null;
        }

        try {
            int hash = hash(key.hashCode());
            DiskStorageFactory.DiskSubstitute<K, V> o = segmentFor(hash).unretrievedGet(key, hash);
            return o;
        } catch (CacheAccessException e) {
            throw new RuntimeException(e);
        }
    }

    Set<? extends K> keySet() {
        return keySet;
    }

    boolean putRawIfAbsent(K key, DiskStorageFactory.DiskMarker<K, V> encoded) {
        try {
            int hash = hash(key.hashCode());
            return segmentFor(hash).putRawIfAbsent(key, hash, encoded);
        } catch (CacheAccessException e) {
            throw new RuntimeException(e);
        }
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

    public static class Provider implements Store.Provider {
        @Override
        public <K, V> DiskStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
            TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[])serviceConfigs);
            TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;

            return new DiskStore<K, V>(storeConfig, "diskstore", timeSource);
        }

        @Override
        public void releaseStore(final Store<?, ?> resource) {
            try {
                resource.clear();
            } catch (CacheAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void start(ServiceConfiguration<?> cfg) {
            // nothing to do
        }

        @Override
        public void stop() {
            // nothing to do
        }
    }
}
