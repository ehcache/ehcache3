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

package org.ehcache.internal.store.disk;

import org.ehcache.Cache;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.ehcache.spi.Persistable;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.internal.store.disk.DiskStorageFactory.Element;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.spi.ServiceLocator.findSingletonAmongst;

/**
 * Implements a persistent-to-disk store.
 * <p/>
 * All new elements are automatically scheduled for writing to disk.
 *
 * @author Ludovic Orban
 */
public class DiskStore<K, V> implements AuthoritativeTier<K, V>, Persistable {

  private static final Logger LOG = LoggerFactory.getLogger(DiskStore.class);

  private static final int ATTEMPT_RATIO = 4;
  private static final int EVICTION_RATIO = 2;
  private static final int DEFAULT_SEGMENT_COUNT = 16;
  private static final int DEFAULT_QUEUE_CAPACITY = 16;
  private static final int DEFAULT_EXPIRY_THREAD_INTERVAL = 30000;

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final TimeSource timeSource;
  private final Expiry<? super K, ? super V> expiry;
  private final Serializer<Element> elementSerializer;
  private final Serializer<Object> indexSerializer;
  private final long capacity;
  private final Predicate<DiskStorageFactory.DiskSubstitute<K, V>> evictionVeto;
  private final Comparator<DiskStorageFactory.DiskSubstitute<K, V>> evictionPrioritizer;
  private final Random random = new Random();
  private final boolean persistent;

  private volatile DiskStorageFactory<K, V> diskStorageFactory;
  private volatile Segment<K, V>[] segments;
  private volatile int segmentShift;

  // TODO: These should not be handled directly by the DiskStore, but through the LocalPersistenceService instead;
  //       Sadly, that's currently not feasible without major refactoring to all this...
  private final File dataFile;
  private final File indexFile;

  public DiskStore(final Configuration<K, V> config, File dataFile, File indexFile, TimeSource timeSource, Serializer<Element> elementSerializer, Serializer<Object> indexSerializer) {
    ResourcePool diskPool = config.getResourcePools().getPoolForResource(ResourceType.Core.DISK);
    if (diskPool == null) {
      throw new IllegalArgumentException("Disk store must be configured with a resource of type 'disk'");
    }
    if (!diskPool.getUnit().equals(EntryUnit.ENTRIES)) {
      throw new IllegalArgumentException("Disk store only handles resource unit 'entries'");
    }
    this.capacity = diskPool.getSize();
    EvictionPrioritizer<? super K, ? super V> prioritizer = config.getEvictionPrioritizer();
    if (prioritizer == null) {
      prioritizer = Eviction.Prioritizer.LRU;
    }
    this.evictionVeto = wrap((Predicate) config.getEvictionVeto());
    this.evictionPrioritizer = (Comparator) wrap((Comparator) prioritizer);
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.timeSource = timeSource;
    this.expiry = config.getExpiry();
    this.elementSerializer = elementSerializer;
    this.indexSerializer = indexSerializer;
    this.dataFile = dataFile;
    this.indexFile = indexFile;
    this.persistent = diskPool.isPersistent();
  }

  private Predicate<DiskStorageFactory.DiskSubstitute<K, V>> wrap(final Predicate<Cache.Entry<K, V>> predicate) {
    if (predicate == null) {
      return Predicates.none();
    } else {
      return new Predicate<DiskStorageFactory.DiskSubstitute<K, V>>() {
        @Override
        public boolean test(DiskStorageFactory.DiskSubstitute<K, V> argument) {
          return predicate.test(wrap(argument));
        }
      };
    }
  }

  private Cache.Entry<K, V> wrap(final DiskStorageFactory.DiskSubstitute<K, V> value) {
    return new Cache.Entry<K, V>() {

      @Override
      public K getKey() {
        return value.getKey();
      }

      @Override
      public V getValue() {
        return getValueHolder().value();
      }

      @Override
      public long getCreationTime(TimeUnit unit) {
        return getValueHolder().creationTime(unit);
      }

      @Override
      public long getLastAccessTime(TimeUnit unit) {
        return getValueHolder().lastAccessTime(unit);
      }

      @Override
      public float getHitRate(TimeUnit unit) {
        return getValueHolder().hitRate(unit);
      }

      private ValueHolder<V> getValueHolder() {
        K key = value.getKey();
        int hash = hash(key.hashCode());
        DiskStorageFactory.Element<K, V> element = segmentFor(hash).get(key, hash, false);
        return element == null ? null : element.getValueHolder();
      }
    };
  }

  private Comparator<DiskStorageFactory.DiskSubstitute<K, V>> wrap(final Comparator<Cache.Entry<K, V>> comparator) {
    return new Comparator<DiskStorageFactory.DiskSubstitute<K, V>>() {
      @Override
      public int compare(DiskStorageFactory.DiskSubstitute<K, V> t, DiskStorageFactory.DiskSubstitute<K, V> u) {
        return comparator.compare(wrap(t), wrap(u));
      }
    };
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
    return internalGetAndFault(key, false);
  }

  ValueHolder<V> internalGetAndFault(final K key, boolean markFaulted) throws CacheAccessException {
    checkKey(key);
    int hash = hash(key.hashCode());

    DiskStorageFactory.Element<K, V> existingElement = segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          return null;
        }

        setAccessTimeAndExpiry(key, mappedValue, now);

        return mappedValue;
      }
    }, Segment.Compute.IF_PRESENT, true, markFaulted);

    return existingElement == null ? null : existingElement.getValueHolder();
  }

  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    checkKey(key);
    int hash = hash(key.hashCode());
    return segmentFor(hash).containsKey(key, hash);
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
    int hash = hash(key.hashCode());
    final long now = timeSource.getTimeMillis();

    final AtomicBoolean entryActuallyAdded = new AtomicBoolean();
    segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        entryActuallyAdded.set(mappedValue == null);

        if (mappedValue != null && mappedValue.isExpired(now)) {
          mappedValue = null;
        }

        if (mappedValue == null) {
          return newCreateValueHolder(key, value, now);
        } else {
          return newUpdateValueHolder(key, mappedValue, value, now);
        }
      }
    }, Segment.Compute.ALWAYS, false, false);

    if (entryActuallyAdded.get()) {
      enforceCapacity(1);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
    int hash = hash(key.hashCode());
    final long now = timeSource.getTimeMillis();

    final AtomicReference<ValueHolder<V>> returnValue = new AtomicReference<ValueHolder<V>>(null);

    segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        if (mappedValue == null || mappedValue.isExpired(now)) {
          return newCreateValueHolder(key, value, now);
        }

        returnValue.set(mappedValue.getValueHolder());
        setAccessTimeAndExpiry(key, mappedValue, now);
        return mappedValue;
      }
    }, Segment.Compute.ALWAYS, false, false);

    return returnValue.get();
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    checkKey(key);
    int hash = hash(key.hashCode());
    segmentFor(hash).remove(key, hash, null);
  }

  @Override
  public boolean remove(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
    int hash = hash(key.hashCode());

    final AtomicBoolean removed = new AtomicBoolean(false);

    segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          return null;
        } else if (value.equals(mappedValue.getValueHolder().value())) {
          removed.set(true);
          return null;
        } else {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
      }
    }, Segment.Compute.IF_PRESENT, false, false);

    return removed.get();
  }

  @Override
  public ValueHolder<V> replace(final K key, final V value) throws CacheAccessException {
    checkKey(key);
    checkValue(value);
    int hash = hash(key.hashCode());

    final AtomicReference<ValueHolder<V>> returnValue = new AtomicReference<ValueHolder<V>>(null);


    segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          return null;
        } else {
          returnValue.set(mappedValue.getValueHolder());
          return newUpdateValueHolder(key, mappedValue, value, now);
        }
      }
    }, Segment.Compute.IF_PRESENT, false, false);

    return returnValue.get();
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) throws CacheAccessException {
    checkKey(key);
    checkValue(oldValue);
    checkValue(newValue);
    int hash = hash(key.hashCode());

    final AtomicBoolean returnValue = new AtomicBoolean(false);


    segmentFor(hash).compute(key, hash, new BiFunction<K, DiskStorageFactory.Element<K, V>, DiskStorageFactory.Element<K, V>>() {
      @Override
      public DiskStorageFactory.Element<K, V> apply(K mappedKey, DiskStorageFactory.Element<K, V> mappedValue) {
        final long now = timeSource.getTimeMillis();

        if (mappedValue.isExpired(now)) {
          return null;
        } else if (oldValue.equals(mappedValue.getValueHolder().value())) {
          returnValue.set(true);
          return newUpdateValueHolder(key, mappedValue, newValue, now);
        } else {
          setAccessTimeAndExpiry(key, mappedValue, now);
          return mappedValue;
        }
      }
    }, Segment.Compute.IF_PRESENT, false, false);

    return returnValue.get();
  }

  @Override
  public void clear() throws CacheAccessException {
    internalClear();
  }

  void internalClear() {
    if (segments != null) {
      for (Segment s : segments) {
        s.clear();
      }
    }
  }

  @Override
  public void destroy() throws Exception {
    internalClear();
    if (dataFile.delete() | indexFile.delete()) {
      LOG.info("Destroyed " + dataFile.getAbsolutePath() + " and " + indexFile.getAbsolutePath());
    }
  }

  @Override
  public boolean isPersistent() {
    return persistent;
  }

  @Override
  public void create() throws Exception {
    boolean dataFileCreated = dataFile.createNewFile();
    boolean indexFileCreated = indexFile.createNewFile();

    if (dataFileCreated && indexFileCreated) {
      LOG.info("Created " + dataFile.getAbsolutePath() + " and " + indexFile.getAbsolutePath());
    } else if (!dataFileCreated && !indexFileCreated) {
      LOG.info("Reusing " + dataFile.getAbsolutePath() + " and " + indexFile.getAbsolutePath());
    } else {
      if (indexFileCreated) {
        dataFile.delete();
        dataFile.createNewFile();
        LOG.warn("Index file " + indexFile.getAbsolutePath() + " was missing, dropped previously persisted data");
      }
      if (dataFileCreated) {
        indexFile.delete();
        indexFile.createNewFile();
        LOG.warn("Data file " + dataFile.getAbsolutePath() + " was missing,  dropped previously persisted data");
      }
    }
  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    //todo: events are missing
  }

  @Override
  public void disableStoreEventNotifications() {
    //todo: events are missing
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    return new DiskStoreIterator();
  }

  @Override
  public ValueHolder<V> getAndFault(K key) throws CacheAccessException {
    return internalGetAndFault(key, true);
  }

  @Override
  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, true);
  }

  @Override
  public boolean flush(K key, ValueHolder<V> valueHolder) {
    if (valueHolder instanceof DiskStorageFactory.DiskValueHolder) {
      throw new IllegalArgumentException("Value holder must be of a class coming from the caching tier");
    }
    int hash = hash(key.hashCode());
    return segmentFor(hash).flush(key, hash, valueHolder);
  }

  class DiskStoreIterator implements Iterator<Cache.Entry<K, ValueHolder<V>>> {
    private final DiskSubstituteIterator diskSubstituteIterator = new DiskSubstituteIterator();
    private DiskStorageFactory.Element<K, V> next;

    DiskStoreIterator() {
      advance();
    }

    private void advance() {
      next = null;
      while (diskSubstituteIterator.hasNext()) {
        DiskStorageFactory.DiskSubstitute<K, V> nextSubstitute = diskSubstituteIterator.next();
        final K key = nextSubstitute.getKey();
        int hash = hash(key.hashCode());
        next = segmentFor(hash).get(key, hash, false);
        if (next != null) {
          break;
        }
      }
    }

    @Override
    public boolean hasNext() throws CacheAccessException {
      return next != null;
    }

    @Override
    public Cache.Entry<K, ValueHolder<V>> next() throws CacheAccessException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      DiskStorageFactory.Element<K, V> element = next;
      advance();

      final K key = element.getKey();
      final ValueHolder<V> valueHolder = element.getValueHolder();
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
    element.getValueHolder().setLastAccessTime(now, DiskStorageFactory.DiskValueHolder.TIME_UNIT);

    DiskStorageFactory.DiskValueHolder<V> valueHolder = element.getValueHolder();
    Duration duration = expiry.getExpiryForAccess(key, valueHolder.value());
    if (duration != null) {
      if (duration.isForever()) {
        valueHolder.setExpirationTime(DiskStorageFactory.DiskValueHolder.NO_EXPIRE, null);
      } else {
        valueHolder.setExpirationTime(safeExpireTime(now, duration), DiskStorageFactory.DiskValueHolder.TIME_UNIT);
      }
    }
  }

  private static long safeExpireTime(long now, Duration duration) {
    long millis = DiskStorageFactory.DiskValueHolder.TIME_UNIT.convert(duration.getAmount(), duration.getTimeUnit());

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

    Duration duration = expiry.getExpiryForUpdate(key, oldValue.getValueHolder().value(), newValue);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration == null) {
      return new DiskStorageFactory.ElementImpl<K, V>(key, newValue, now, oldValue.getValueHolder().expirationTime(DiskStorageFactory.DiskValueHolder.TIME_UNIT));
    } else {
      if (duration.isForever()) {
        return new DiskStorageFactory.ElementImpl<K, V>(key, newValue, now, DiskStorageFactory.DiskValueHolder.NO_EXPIRE);
      } else {
        return new DiskStorageFactory.ElementImpl<K, V>(key, newValue, now, safeExpireTime(now, duration));
      }
    }
  }

  private DiskStorageFactory.Element<K, V> newCreateValueHolder(K key, V value, long now) {
    if (value == null) {
      throw new NullPointerException();
    }

    Duration duration = expiry.getExpiryForCreation(key, value);
    if (Duration.ZERO.equals(duration)) {
      return null;
    }

    if (duration.isForever()) {
      return new DiskStorageFactory.ElementImpl<K, V>(key, value, now, DiskStorageFactory.DiskValueHolder.NO_EXPIRE);
    } else {
      return new DiskStorageFactory.ElementImpl<K, V>(key, value, now, safeExpireTime(now, duration));
    }
  }

  ValueHolder<V> enforceCapacityIfValueNotNull(final DiskStorageFactory.Element<K, V> computeResult) {
    if (computeResult != null) {
      enforceCapacity(1);
    }
    return computeResult == null ? null : computeResult.getValueHolder();
  }

  void enforceCapacity(int delta) {
    for (int attempts = 0, evicted = 0; attempts < ATTEMPT_RATIO * delta && evicted < EVICTION_RATIO * delta
        && capacity < size(); attempts++) {
      evicted += diskStorageFactory.evict(1);
    }
  }

  DiskStorageFactory.Element<K, V> evict(K key, DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
    return evictElement(key, diskSubstitute);
  }

  DiskStorageFactory.Element<K, V> expire(K key, DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
    return evictElement(key, diskSubstitute);
  }


  private DiskStorageFactory.Element<K, V> evictElement(K key, DiskStorageFactory.DiskSubstitute<K, V> diskSubstitute) {
    int hash = hash(key.hashCode());
    return segmentFor(hash).evict(key, hash, diskSubstitute);
  }

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

        V existingValue = mappedElement == null ? null : mappedElement.getValueHolder().value();
        V computedValue = mappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          return null;
        } else if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
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

    DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.ALWAYS, false, false);
    return enforceCapacityIfValueNotNull(computedElement);
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    return internalComputeIfAbsent(key, mappingFunction, false);
  }

  private ValueHolder<V> internalComputeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction, boolean fault) throws CacheAccessException {
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
    DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.IF_ABSENT, false, fault);
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

        V existingValue = mappedElement == null ? null : mappedElement.getValueHolder().value();

        V computedValue = remappingFunction.apply(mappedKey, existingValue);
        if (computedValue == null) {
          return null;
        }

        if ((eq(existingValue, computedValue)) && (!replaceEqual.apply())) {
          setAccessTimeAndExpiry(key, mappedElement, now);
          return mappedElement;
        }

        checkValue(computedValue);
        return newUpdateValueHolder(key, mappedElement, computedValue, now);
      }
    };

    DiskStorageFactory.Element<K, V> computedElement = segmentFor(hash).compute(key, hash, biFunction, Segment.Compute.IF_PRESENT, false, false);
    return computedElement == null ? null : computedElement.getValueHolder();
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

  public void flushToDisk() throws ExecutionException, InterruptedException {
    diskStorageFactory.flush().get();
    diskStorageFactory.evictToSize();
  }

  boolean fault(K key, DiskStorageFactory.Placeholder<K, V> expect, DiskStorageFactory.DiskMarker<K, V> fault) {
    int hash = hash(key.hashCode());
    return segmentFor(hash).fault(key, hash, expect, fault, false);
  }

  DiskStorageFactory.DiskSubstitute<K, V> unretrievedGet(K key) {
    if (key == null) {
      return null;
    }

    int hash = hash(key.hashCode());
    DiskStorageFactory.DiskSubstitute<K, V> o = segmentFor(hash).unretrievedGet(key, hash);
    return o;
  }

  java.util.Iterator<DiskStorageFactory.DiskSubstitute<K, V>> diskSubstituteIterator() {
    return new DiskSubstituteIterator();
  }

  boolean putRawIfAbsent(K key, DiskStorageFactory.DiskMarker<K, V> encoded) {
    int hash = hash(key.hashCode());
    return segmentFor(hash).putRawIfAbsent(key, hash, encoded);
  }

  /**
   * Select a random sample of elements generated by the supplied factory.
   *
   * @param factory    generator of the given type
   * @param sampleSize minimum number of elements to return
   * @param keyHint    a key on which we are currently working
   * @return list of sampled elements/element substitute
   */
  public List<DiskStorageFactory.DiskSubstitute<K, V>> getRandomSample(ElementSubstituteFilter factory, int sampleSize, Object keyHint) {
    ArrayList<DiskStorageFactory.DiskSubstitute<K, V>> sampled = new ArrayList<DiskStorageFactory.DiskSubstitute<K, V>>(sampleSize);

    // pick a random starting point in the map
    int randomHash = random.nextInt();

    final int segmentStart;
    if (keyHint == null) {
      segmentStart = (randomHash >>> segmentShift);
    } else {
      segmentStart = (hash(keyHint.hashCode()) >>> segmentShift);
    }

    int segmentIndex = segmentStart;
    do {
      segments[segmentIndex].addRandomSample(factory, sampleSize, sampled, randomHash);
      if (sampled.size() >= sampleSize) {
        break;
      }

      // move to next segment
      segmentIndex = (segmentIndex + 1) & (segments.length - 1);
    } while (segmentIndex != segmentStart);

    return sampled;
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

  class DiskSubstituteIterator extends HashIterator implements java.util.Iterator<DiskStorageFactory.DiskSubstitute<K, V>> {
    /**
     * {@inheritDoc}
     */
    public DiskStorageFactory.DiskSubstitute<K, V> next() {
      return super.nextEntry().element;
    }
  }

  @SupplementaryService
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private volatile ServiceProvider serviceProvider;
    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());

    @Override
    public <K, V> DiskStore<K, V> createStore(final Configuration<K, V> storeConfig, final ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new RuntimeException("ServiceProvider is null.");
      }
      TimeSourceConfiguration timeSourceConfig = findSingletonAmongst(TimeSourceConfiguration.class, (Object[]) serviceConfigs);
      TimeSource timeSource = timeSourceConfig != null ? timeSourceConfig.getTimeSource() : SystemTimeSource.INSTANCE;
      
      SerializationProvider serializationProvider = serviceProvider.findService(SerializationProvider.class);
      Serializer<Element> elementSerializer = serializationProvider.createSerializer(Element.class, storeConfig.getClassLoader());
      Serializer<Object> objectSerializer = serializationProvider.createSerializer(Object.class, storeConfig.getClassLoader());
      
      // todo: This should be enforced at the type system
      Object identifier;
      if(storeConfig instanceof PersistentStoreConfiguration<?, ?, ?>) {
        identifier = ((PersistentStoreConfiguration) storeConfig).getIdentifier();
      } else {
        throw new IllegalArgumentException("Store.Configuration for DiskStore should implement Store.PersistentStoreConfiguration");
      }

      LocalPersistenceService localPersistenceService = serviceProvider.findService(LocalPersistenceService.class);

      DiskStore<K, V> diskStore = new DiskStore<K, V>(storeConfig, localPersistenceService.getDataFile(identifier),
          localPersistenceService.getIndexFile(identifier), timeSource, elementSerializer, objectSerializer);
      createdStores.add(diskStore);
      return diskStore;
    }

    @Override
    public void releaseStore(final Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      close((DiskStore)resource);
    }

    static void close(final DiskStore resource) {
      if (resource.diskStorageFactory == null) {
        LOG.warn("disk store already closed");
        return;
      }
      resource.diskStorageFactory.unbind();
      resource.diskStorageFactory = null;
      resource.segments = null;
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      init((DiskStore)resource);
    }

    static void init(final DiskStore resource) {
      try {
        resource.diskStorageFactory = new DiskStorageFactory<Object, Object>(resource.capacity, resource.evictionVeto, resource.evictionPrioritizer,
            resource.timeSource, resource.elementSerializer, resource.indexSerializer, resource.dataFile, resource.indexFile,
            DEFAULT_SEGMENT_COUNT, DEFAULT_QUEUE_CAPACITY, DEFAULT_EXPIRY_THREAD_INTERVAL);
      } catch (FileNotFoundException fnfe) {
        throw new IllegalStateException(fnfe);
      }

      resource.segments = new Segment[DEFAULT_SEGMENT_COUNT];
      for (int i = 0; i < resource.segments.length; i++) {
        resource.segments[i] = new Segment<Object, Object>(resource.diskStorageFactory, resource.timeSource, resource);
      }

      resource.segmentShift = Integer.numberOfLeadingZeros(resource.segments.length - 1);

      resource.diskStorageFactory.bind(resource);
    }

    @Override
    public void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
      this.serviceProvider = serviceProvider;
    }

    @Override
    public void stop() {
      this.serviceProvider = null;
      createdStores.clear();
    }

    @Override
    public <K, V> AuthoritativeTier<K, V> createAuthoritativeTier(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStore(storeConfig, serviceConfigs);
    }

    @Override
    public void releaseAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      releaseStore(resource);
    }

    @Override
    public void initAuthoritativeTier(AuthoritativeTier<?, ?> resource) {
      initStore(resource);
    }
  }
}
