///*
// * Copyright Terracotta, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.ehcache.impl.internal.store.shared;
//
//import org.ehcache.Cache;
//import org.ehcache.config.EvictionAdvisor;
//import org.ehcache.core.CacheConfigurationChangeListener;
//import org.ehcache.core.Ehcache;
//import org.ehcache.core.spi.service.StatisticsService;
//import org.ehcache.core.spi.store.AbstractValueHolder;
//import org.ehcache.core.spi.store.ConfigurationChangeSupport;
//import org.ehcache.core.spi.store.Store;
//import org.ehcache.core.spi.store.events.StoreEventSource;
//import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
//import org.ehcache.core.spi.store.tiering.CachingTier;
//import org.ehcache.core.spi.store.tiering.LowerCachingTier;
//import org.ehcache.expiry.ExpiryPolicy;
//import org.ehcache.impl.store.BaseStore;
//import org.ehcache.spi.resilience.StoreAccessException;
//import org.ehcache.spi.serialization.Serializer;
//import org.ehcache.spi.serialization.SerializerException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nonnull;
//import java.nio.ByteBuffer;
//import java.time.Duration;
//import java.util.*;
//import java.util.function.BiFunction;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.function.Supplier;
//
//@SuppressWarnings({"unchecked", "rawtypes"})
//public class StorePartition<K, V> extends BaseStore<K, V> implements Store<K, V>, ConfigurationChangeSupport, AuthoritativeTier<K, V>, LowerCachingTier<K, V> {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(StorePartition.class);
//
//  private final int id;
//  private final Store<CompositeValue<K>, CompositeValue<V>> store;
//  private final Map<Integer, InvalidationValve> invalidationValveMap;
//  private final Map<Integer, CachingTier.InvalidationListener> invalidationListenerMap;
//  private static final Supplier<Boolean> SUPPLY_TRUE = () -> Boolean.TRUE;
//
////  private final OperationObserver<StoreOperationOutcomes.GetOutcome> getObserver;
////  private final OperationObserver<StoreOperationOutcomes.PutOutcome> putObserver;
////  private final OperationObserver<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsentObserver;
////  private final OperationObserver<StoreOperationOutcomes.RemoveOutcome> removeObserver;
////  private final OperationObserver<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemoveObserver;
////  private final OperationObserver<StoreOperationOutcomes.ReplaceOutcome> replaceObserver;
////  private final OperationObserver<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplaceObserver;
////  private final OperationObserver<StoreOperationOutcomes.ComputeOutcome> computeObserver;
////  private final OperationObserver<StoreOperationOutcomes.ComputeIfAbsentOutcome> computeIfAbsentObserver;
////  private final OperationObserver<StoreOperationOutcomes.EvictionOutcome> evictionObserver;
////  private final OperationObserver<StoreOperationOutcomes.ExpirationOutcome> expirationObserver;
////  private final OperationObserver<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome> getAndFaultObserver;
////  private final OperationObserver<AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome> computeIfAbsentAndFaultObserver;
////  private final OperationObserver<AuthoritativeTierOperationOutcomes.FlushOutcome> flushObserver;
////  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateOutcome> invalidateObserver;
////  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateAllOutcome> invalidateAllObserver;
////  private final OperationObserver<LowerCachingTierOperationsOutcome.InvalidateAllWithHashOutcome> invalidateAllWithHashObserver;
////  private final OperationObserver<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome> getAndRemoveObserver;
////  private final OperationObserver<LowerCachingTierOperationsOutcome.InstallMappingOutcome> installMappingObserver;
//
//  public StorePartition(int id, Class<K> keyType, Class<V> valueType,
//                        Store<CompositeValue<?>, CompositeValue<?>> store,
//                        Map<Integer, InvalidationValve> invalidationValveMap,
//                        Map<Integer, CachingTier.InvalidationListener> invalidationListenerMap,
//                        StatisticsService statisticsService,
//                        boolean operationStatisticsEnabled) {
//    super(keyType, valueType, operationStatisticsEnabled, statisticsService);
//    this.id = id;
//    this.store = (Store) store;
//    this.invalidationValveMap = invalidationValveMap;
//    this.invalidationListenerMap = invalidationListenerMap;
//
////    this.getObserver = createObserver("get", StoreOperationOutcomes.GetOutcome.class, true);
////    this.putObserver = createObserver("put", StoreOperationOutcomes.PutOutcome.class, true);
////    this.putIfAbsentObserver = createObserver("putIfAbsent", StoreOperationOutcomes.PutIfAbsentOutcome.class, true);
////    this.removeObserver = createObserver("remove", StoreOperationOutcomes.RemoveOutcome.class, true);
////    this.conditionalRemoveObserver = createObserver("conditionalRemove", StoreOperationOutcomes.ConditionalRemoveOutcome.class, true);
////    this.replaceObserver = createObserver("replace", StoreOperationOutcomes.ReplaceOutcome.class, true);
////    this.conditionalReplaceObserver = createObserver("conditionalReplace", StoreOperationOutcomes.ConditionalReplaceOutcome.class, true);
////    this.computeObserver = createObserver("compute", StoreOperationOutcomes.ComputeOutcome.class, true);
////    this.computeIfAbsentObserver = createObserver("computeIfAbsent", StoreOperationOutcomes.ComputeIfAbsentOutcome.class, true);
////    this.evictionObserver = createObserver("eviction", StoreOperationOutcomes.EvictionOutcome.class, false);
////    this.expirationObserver = createObserver("expiration", StoreOperationOutcomes.ExpirationOutcome.class, false);
////
////    this.getAndFaultObserver = createObserver("getAndFault", AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.class, true);
////    this.computeIfAbsentAndFaultObserver = createObserver("computeIfAbsentAndFault", AuthoritativeTierOperationOutcomes.ComputeIfAbsentAndFaultOutcome.class, true);
////    this.flushObserver = createObserver("flush", AuthoritativeTierOperationOutcomes.FlushOutcome.class, true);
////
////    this.invalidateObserver = createObserver("invalidate", LowerCachingTierOperationsOutcome.InvalidateOutcome.class, true);
////    this.invalidateAllObserver = createObserver("invalidateAll", LowerCachingTierOperationsOutcome.InvalidateAllOutcome.class, true);
////    this.invalidateAllWithHashObserver = createObserver("invalidateAllWithHash", LowerCachingTierOperationsOutcome.InvalidateAllWithHashOutcome.class, true);
////    this.getAndRemoveObserver= createObserver("getAndRemove", LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.class, true);
////    this.installMappingObserver= createObserver("installMapping", LowerCachingTierOperationsOutcome.InstallMappingOutcome.class, true);
//  }
//
//  @Override
//  public ValueHolder<V> get(K key) throws StoreAccessException {
//    checkKey(key);
//    return decode(store.get(compositeKey(key)));
//  }
//
//  @Override
//  public boolean containsKey(K key) throws StoreAccessException {
//    checkKey(key);
//    return store.containsKey(compositeKey(key));
//  }
//
//  @Override
//  public PutStatus put(K key, V value) throws StoreAccessException {
//    checkKey(key);
//    checkValue(value);
//    return store.put(compositeKey(key), compositeValue(value));
//  }
//
//  @Override
//  public ValueHolder<V> getAndPut(K key, V value) throws StoreAccessException {
//    checkKey(key);
//    checkValue(value);
//    return decode(store.getAndPut(compositeKey(key), compositeValue(value)));
//  }
//
//  @Override
//  public ValueHolder<V> putIfAbsent(K key, V value, Consumer<Boolean> put) throws StoreAccessException {
//    checkKey(key);
//    checkValue(value);
//    return decode(store.putIfAbsent(compositeKey(key), compositeValue(value), put));
//  }
//
//  @Override
//  public boolean remove(K key) throws StoreAccessException {
//    checkKey(key);
//    return store.remove(compositeKey(key));
//  }
//
//  @Override
//  public ValueHolder<V> getAndRemove(K key) throws StoreAccessException {
//    checkKey(key);
//    return decode(store.getAndRemove(compositeKey(key)));
//  }
//
//  @Override
//  public RemoveStatus remove(K key, V value) throws StoreAccessException {
//    checkKey(key);
//    checkValue(value);
//    return store.remove(compositeKey(key), compositeValue(value));
//  }
//
//  @Override
//  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
//    checkKey(key);
//    checkValue(value);
//    return decode(store.replace(compositeKey(key), compositeValue(value)));
//  }
//
//  @Override
//  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
//    checkKey(key);
//    checkValue(oldValue);
//    checkValue(newValue);
//    return store.replace(compositeKey(key), compositeValue(oldValue), compositeValue(newValue));
//  }
//
//  @Override
//  public ValueHolder<V> getAndCompute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
//    checkKey(key);
//    return decode(store.getAndCompute(compositeKey(key), new EncodedMappingBiFunction(id, keyType, valueType, mappingFunction)));
//  }
//
//  @Override
//  public ValueHolder<V> computeAndGet(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter) throws StoreAccessException {
//    checkKey(key);
//    return decode(store.computeAndGet(compositeKey(key), new EncodedMappingBiFunction(id, keyType, valueType, mappingFunction), replaceEqual, invokeWriter));
//  }
//
//  @Override
//  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
//    checkKey(key);
//    return decode(store.computeIfAbsent(compositeKey(key), new EncodedMappingFunction(id, keyType, valueType, mappingFunction)));
//  }
//
//  @Override
//  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys,
//                                            Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
//    return bulkCompute(keys, remappingFunction, SUPPLY_TRUE);
//  }
//
//  @Override
//  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys,
//                                            Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction,
//                                            Supplier<Boolean> replaceEqual) throws StoreAccessException {
//    Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> results;
//    Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
//    if (remappingFunction instanceof Ehcache.PutAllFunction) {
//      Ehcache.PutAllFunction<K, V> putAllFunction = (Ehcache.PutAllFunction<K, V>) remappingFunction;
//      Map<CompositeValue<K>, CompositeValue<V>> encodedEntriesToRemap = new HashMap<>();
//      putAllFunction.getEntriesToRemap().forEach((k, v) -> encodedEntriesToRemap.put(compositeKey(k), compositeValue(v)));
//      Ehcache.PutAllFunction<CompositeValue<K>, CompositeValue<V>> encodedRemappingFunction = new Ehcache.PutAllFunction(putAllFunction.getLogger(), encodedEntriesToRemap, putAllFunction.getExpiry());
//      results = store.bulkCompute(compositeKeys(keys), encodedRemappingFunction);
//      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
//      putAllFunction.setActualPutCount(encodedRemappingFunction.getActualPutCount());
//      putAllFunction.setActualUpdateCount(encodedRemappingFunction.getActualUpdateCount());
//    } else if (remappingFunction instanceof Ehcache.RemoveAllFunction) {
//      Ehcache.RemoveAllFunction<K, V> removeAllFunction = (Ehcache.RemoveAllFunction<K, V>) remappingFunction;
//      Ehcache.RemoveAllFunction<CompositeValue<K>, CompositeValue<V>> encodedRemappingFunction = new Ehcache.RemoveAllFunction<>();
//      results = store.bulkCompute(compositeKeys(keys), encodedRemappingFunction);
//      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
//      removeAllFunction.setActualRemoveCount(encodedRemappingFunction.getActualRemoveCount());
//    } else {
//      results = store.bulkCompute(compositeKeys(keys), new BulkComputeMappingFunction(id, keyType, valueType, remappingFunction));
//      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
//    }
//    return decodedResults;
//  }
//
//  @Override
//  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys,
//                                                    Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
//      Map<K, ValueHolder<V>> decodedResults = new HashMap<>();
//      Map<CompositeValue<K>, ValueHolder<CompositeValue<V>>> results =
//        store.bulkComputeIfAbsent(compositeKeys(keys), new BulkComputeIfAbsentMappingFunction(id, keyType, valueType, mappingFunction));
//      results.forEach((k, v) -> decodedResults.put(k.getValue(), decode(v)));
//      return decodedResults;
//  }
//
//  @Override
//  public void clear() throws StoreAccessException {
//    boolean completeRemoval = true;
//    Iterator<Cache.Entry<K, ValueHolder<V>>> iterator = iterator();
//    while (iterator.hasNext()) {
//      try {
//        store.remove(compositeKey(iterator.next().getKey()));
//      } catch (StoreAccessException cae) {
//        completeRemoval = false;
//      }
//    }
//    if (!completeRemoval) {
//      LOGGER.error("Iteration failures may have prevented a complete removal");
//    }
//  }
//
//  @Override
//  public StoreEventSource<K, V> getStoreEventSource() {
//    return (StoreEventSource<K, V>) store.getStoreEventSource();
//  }
//
//  @Override
//
//  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
//
//    Iterator<Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>>> iterator = store.iterator();
//    return new Iterator<Cache.Entry<K, ValueHolder<V>>>() {
//      private Cache.Entry<K, ValueHolder<V>> prefetched = advance();
//      @Override
//      public boolean hasNext() {
//        return prefetched != null;
//      }
//
//      @Override
//      public Cache.Entry<K, ValueHolder<V>> next() {
//        if (prefetched == null) {
//          throw new NoSuchElementException();
//        } else {
//          Cache.Entry<K, ValueHolder<V>> next = prefetched;
//          prefetched = advance();
//          return next;
//        }
//      }
//
//      private Cache.Entry<K, ValueHolder<V>> advance() {
//        while (iterator.hasNext()) {
//          try {
//            Cache.Entry<CompositeValue<K>, ValueHolder<CompositeValue<V>>> next = iterator.next();
//            if (next.getKey().storeId == id) {
//              return new Cache.Entry<K, ValueHolder<V>>() {
//                @Override
//                public K getKey() {
//                  return next.getKey().getValue();
//                }
//                @Override
//                public ValueHolder<V> getValue() {
//                  return decode(next.getValue());
//                }
//              };
//            }
//          } catch (StoreAccessException ex) {
//            throw new RuntimeException(ex);
//          }
//        }
//        return null;
//      }
//    };
//  }
//
//  private CompositeValue<K> compositeKey(K key) {
//    return new CompositeValue<>(id, key);
//  }
//
//  private CompositeValue<V> compositeValue(V value) {
//    return new CompositeValue<>(id, value);
//  }
//
//  private ValueHolder<V> decode(ValueHolder<CompositeValue<V>> value) {
//    return value == null ? null : new DecodedValueHolder<>(value);
//  }
//
//  private ValueHolder<CompositeValue<V>> encode(int storeId, ValueHolder<V> value) {
//    return value == null ? null : new EncodedValueHolder<>(storeId, value);
//  }
//
//  private Set<CompositeValue<K>> compositeKeys(Set<? extends K> keys) {
//    Set<CompositeValue<K>> compositeKeys = new HashSet<>();
//    keys.forEach(k -> compositeKeys.add(compositeKey(k)));
//    return compositeKeys;
//  }
//
//  // ConfigurationChangeSupport
//
//  @Override
//  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
//    return store.getConfigurationChangeListeners();
//  }
//
//  // AuthoritativeTier
//
//  @Override
//  public ValueHolder<V> getAndFault(K key) throws StoreAccessException {
//    checkKey(key);
//    return decode(((AuthoritativeTier<CompositeValue<K>, CompositeValue<V>>) store).getAndFault(compositeKey(key)));
//  }
//
//  @Override
//  public ValueHolder<V> computeIfAbsentAndFault(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
//    checkKey(key);
//    return decode(((AuthoritativeTier) store).computeIfAbsentAndFault(compositeKey(key), new EncodedMappingFunction(id, keyType, valueType, mappingFunction)));
//  }
//
//  @Override
//  public boolean flush(K key, ValueHolder<V> valueHolder) {
//    checkKey(key); // do not call checkValue (i.e. nulls allowed)
//    return ((AuthoritativeTier) store).flush(compositeKey(key), decode((ValueHolder<CompositeValue<V>>) valueHolder));
//  }
//
//  @Override
//  public void setInvalidationValve(InvalidationValve valve) {
//    invalidationValveMap.put(id, valve);
//  }
//
//  @Override
//  public Iterable<? extends Map.Entry<? extends K, ? extends ValueHolder<V>>> bulkComputeIfAbsentAndFault(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
//    return bulkComputeIfAbsent((Set<? extends K>) keys, mappingFunction).entrySet();
//  }
//
//  // LowerCachingTier
//
//  @Override
//  public ValueHolder<V> installMapping(K key, Function<K, ValueHolder<V>> source) throws StoreAccessException {
//    checkKey(key);
//    return decode(((LowerCachingTier) store).installMapping(compositeKey(key), new EncodedInstallMappingFunction(id, keyType, valueType, source)));
//  }
//
//  @Override
//  public void invalidate(K key) throws StoreAccessException {
//    checkKey(key);
//    ((LowerCachingTier) store).invalidate(compositeKey(key));
//  }
//
//  @Override
//  public void invalidateAll() throws StoreAccessException {
//    Exception exception = null;
//    long errorCount = 0;
//    Iterator<Cache.Entry<K, ValueHolder<V>>> iterator = iterator();
//    while (iterator.hasNext()) {
//      try {
//        ((LowerCachingTier) store).invalidate(compositeKey(iterator.next().getKey()));
//      } catch (Exception ex) {
//        errorCount++;
//        if (exception == null) {
//          exception = ex;
//        }
//      }
//    }
//    if (exception != null) {
//      throw new StoreAccessException("invalidateAll failed - error count: " + errorCount, exception);
//    }
//  }
//
//  @Override
//  public void setInvalidationListener(CachingTier.InvalidationListener<K, V> invalidationListener) {
//    invalidationListenerMap.put(id, invalidationListener);
//  }
//
//  @Override
//  public void invalidateAllWithHash(long hash) throws StoreAccessException {
//    ((LowerCachingTier) store).invalidateAllWithHash(hash);
//  }
//
//  public String getStatisticsTag() {
//    return "StorePartition";
//  }
//
//  public static class CompositeValue<T> {
//    private final int storeId;
//    private final T value;
//
//    public CompositeValue(int storeId, T value) {
//      this.storeId = storeId;
//      this.value = value;
//    }
//
//    public int getStoreId() {
//      return storeId;
//    }
//
//    public T getValue() {
//      return value;
//    }
//
//    @Override
//    public int hashCode() {
//      return storeId * 31 + value.hashCode();
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) return true;
//      if (o == null || getClass() != o.getClass()) return false;
//      CompositeValue<?> that = (CompositeValue<?>) o;
//      return storeId == that.storeId && Objects.equals(value, that.value);
//    }
//  }
//
//  public static class CompositeSerializer<T> implements Serializer<CompositeValue<T>> {
//    private final Map<Integer, Serializer<?>> serializerMap;
//
//    public CompositeSerializer(Map<Integer, Serializer<?>> serializerMap) {
//      this.serializerMap = serializerMap;
//    }
//
//    @Override
//    public ByteBuffer serialize(CompositeValue<T> compositeValue) throws SerializerException {
//      int id = compositeValue.getStoreId();
//      ByteBuffer valueForm = ((Serializer<Object>) serializerMap.get(id)).serialize(compositeValue.getValue());
//      ByteBuffer compositeForm = ByteBuffer.allocate(4 + valueForm.remaining());
//      compositeForm.putInt(compositeValue.getStoreId());
//      compositeForm.put(valueForm);
//      compositeForm.flip();
//      return compositeForm;
//    }
//
//    @Override
//    public CompositeValue<T> read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
//      int id = binary.getInt();
//      Serializer<T> serializer = (Serializer<T>) serializerMap.get(id);
//      return new CompositeValue<>(id, serializer.read(binary));
//    }
//
//    @Override
//    public boolean equals(CompositeValue<T> object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
//      return object.equals(read(binary));
//    }
//  }
//
//  public static class CompositeExpiryPolicy<K, V> implements ExpiryPolicy<CompositeValue<K>, CompositeValue<V>> {
//    private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap;
//
//    public CompositeExpiryPolicy(Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap) {
//      this.expiryPolicyMap = expiryPolicyMap;
//    }
//
//    @Override
//    public Duration getExpiryForCreation(CompositeValue<K> key, CompositeValue<V> value) {
//      ExpiryPolicy expiryPolicy = expiryPolicyMap.get(key.getStoreId());
//      return expiryPolicy.getExpiryForCreation(key.getValue(), value.getValue());
//    }
//
//    @Override
//    public Duration getExpiryForAccess(CompositeValue<K> key, Supplier<? extends CompositeValue<V>> value) {
//      ExpiryPolicy expiryPolicy = expiryPolicyMap.get(key.getStoreId());
//      return expiryPolicy.getExpiryForAccess(key.getValue(), new ExpirySupplier(value));
//    }
//
//    @Override
//    public Duration getExpiryForUpdate(CompositeValue<K> key, Supplier<? extends CompositeValue<V>> oldValue, CompositeValue<V> newValue) {
//      ExpiryPolicy expiryPolicy = expiryPolicyMap.get(key.getStoreId());
//      return expiryPolicy.getExpiryForUpdate(key.getValue(), new ExpirySupplier(oldValue), newValue.getValue());
//    }
//  }
//
//  public static class CompositeEvictionAdvisor implements EvictionAdvisor<CompositeValue<?>, CompositeValue<?>> {
//    private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap;
//
//    public CompositeEvictionAdvisor(Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap) {
//      this.evictionAdvisorMap = evictionAdvisorMap;
//    }
//
//    @Override
//    public boolean adviseAgainstEviction(CompositeValue<?> key, CompositeValue<?> value) {
//      return ((EvictionAdvisor)evictionAdvisorMap.get(key.storeId)).adviseAgainstEviction(key.getValue(), value.getValue());
//    }
//  }
//
//  public static class CompositeInvalidationValve implements InvalidationValve {
//
//    private final Map<Integer, InvalidationValve> invalidationValveMap;
//
//    public CompositeInvalidationValve(Map<Integer, InvalidationValve> invalidationValveMap) {
//      this.invalidationValveMap = invalidationValveMap;
//    }
//
//    @Override
//    public void invalidateAll() throws StoreAccessException {
//      invalidationValveMap.forEach((k,v) -> {
//        try {
//          v.invalidateAll();  // how to tell which storeId to use???
//        } catch (StoreAccessException e) {
//          throw new RuntimeException(e);
//        }
//      });
//    }
//
//    @Override
//    public void invalidateAllWithHash(long hash) throws StoreAccessException {
//      invalidationValveMap.forEach((k,v) -> {
//        try {
//          v.invalidateAllWithHash(hash); // correct the hash
//        } catch (StoreAccessException e) {
//          throw new RuntimeException(e);
//        }
//      });
//    }
//  }
//
//  public static class CompositeInvalidationListener implements CachingTier.InvalidationListener {
//
//    private final Map<Integer, CachingTier.InvalidationListener> invalidationListenerMap;
//
//    public CompositeInvalidationListener(Map<Integer, CachingTier.InvalidationListener> invalidationListenerMap) {
//      this.invalidationListenerMap = invalidationListenerMap;
//    }
//
//    @Override
//    public void onInvalidation(Object key, ValueHolder valueHolder) {
//      CachingTier.InvalidationListener listener = invalidationListenerMap.get(((CompositeValue) key).getStoreId());
//      if (listener != null) {
//        listener.onInvalidation(((CompositeValue<?>) key).getValue(), new DecodedValueHolder(valueHolder));
//      }
//    }
//  }
//
//  public static class DecodedValueHolder<T> extends AbstractValueHolder<T> implements ValueHolder<T> {
//    private final ValueHolder<CompositeValue<T>> compositeValueHolder;
//
//    public DecodedValueHolder(ValueHolder<CompositeValue<T>> compositeValueHolder) {
//      super(compositeValueHolder.getId(), compositeValueHolder.creationTime(), compositeValueHolder.expirationTime());
//      setLastAccessTime(compositeValueHolder.lastAccessTime());
//      this.compositeValueHolder = compositeValueHolder;
//    }
//
//    @Override
//    @Nonnull
//    public T get() {
//      return compositeValueHolder.get().value;
//    }
//  }
//
//  public static class EncodedValueHolder<T> implements ValueHolder<CompositeValue<T>> {
//    private final ValueHolder<T> valueHolder;
//    private final int storeId;
//
//    public EncodedValueHolder(int storeId, ValueHolder<T> valueHolder) {
//      this.valueHolder = valueHolder;
//      this.storeId = storeId;
//    }
//
//    @Override
//    @Nonnull
//    public CompositeValue<T> get() {
//      return new CompositeValue(storeId, valueHolder.get());
//    }
//
//    @Override
//    public long creationTime() {
//      return valueHolder.creationTime();
//    }
//
//    @Override
//    public long expirationTime() {
//      return valueHolder.expirationTime();
//    }
//
//    @Override
//    public boolean isExpired(long expirationTime) {
//      return valueHolder.isExpired(expirationTime);
//    }
//
//    @Override
//    public long lastAccessTime() {
//      return valueHolder.lastAccessTime();
//    }
//
//    @Override
//    public long getId() {
//      return valueHolder.getId();
//    }
//  }
//
//  public static class ExpirySupplier<V> implements Supplier<V> {
//    private final Supplier<CompositeValue<V>> supplier;
//    ExpirySupplier (Supplier<CompositeValue<V>> supplier) {
//      this.supplier = supplier;
//    }
//    @Override
//    public V get() {
//      return supplier.get().getValue();
//    }
//  }
//
//  public static abstract class BaseRemappingFunction<K,V> {
//    protected final int storeId;
//    protected final Class<K> keyType;
//    protected final Class<V> valueType;
//
//    BaseRemappingFunction(int storeId, Class<K> keyType, Class<V> valueType) {
//      this.storeId = storeId;
//      this.keyType = keyType;
//      this.valueType = valueType;
//    }
//
//    protected void keyCheck(Object keyObject) {
//      if (!keyType.isInstance(Objects.requireNonNull(keyObject))) {
//        throw new ClassCastException("Invalid key type, expected : " + keyType.getName() + " but was : " + keyObject.getClass().getName());
//      }
//    }
//
//    protected void valueCheck(Object valueObject) {
//      if (!valueType.isInstance(Objects.requireNonNull(valueObject))) {
//        throw new ClassCastException("Invalid value type, expected : " + valueType.getName() + " but was : " + valueObject.getClass().getName());
//      }
//    }
//  }
//  public static class EncodedInstallMappingFunction<K, V> extends BaseRemappingFunction implements Function<CompositeValue<K>, ValueHolder<CompositeValue<V>>> {
//    private final Function<K, ValueHolder<V>> function;
//
//    EncodedInstallMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<K, ValueHolder<V>> function) {
//      super(storeId, keyType, valueType);
//      this.function = function;
//    }
//
//    @Override
//    public ValueHolder<CompositeValue<V>> apply(CompositeValue<K> key) {
//      keyCheck(key.getValue());
//      ValueHolder<V> value = function.apply(key.getValue());
//      return value == null ? null : new EncodedValueHolder(storeId, value);
//    }
//  }
//
//  public static class EncodedMappingFunction<K, V> extends BaseRemappingFunction implements Function<CompositeValue<K>, CompositeValue<V>> {
//    private final Function<K, V> function;
//
//    EncodedMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<K, V> function) {
//      super(storeId, keyType, valueType);
//      this.function = function;
//    }
//    @Override
//    public CompositeValue<V> apply(CompositeValue<K> key) {
//      keyCheck(key.getValue());
//      V value = function.apply(key.getValue());
//      return value == null ? null : new CompositeValue(storeId, value);
//    }
//  }
//
//  public static class EncodedMappingBiFunction<K, V> extends BaseRemappingFunction implements BiFunction<CompositeValue<K>, CompositeValue<V>, CompositeValue<V>> {
//    private final BiFunction<K, V, V> function;
//
//    EncodedMappingBiFunction(int storeId, Class<K> keyType, Class<V> valueType, BiFunction<K, V, V> function) {
//      super(storeId, keyType, valueType);
//      this.function = function;
//    }
//
//    @Override
//    public CompositeValue<V> apply(CompositeValue<K> key, CompositeValue<V> value) {
//      keyCheck(key.getValue());
//      V val = function.apply(key.getValue(), value != null ? value.getValue() : null);
//      return val == null ? null : new CompositeValue(storeId, val);
//    }
//  }
//
//  public static class BulkComputeMappingFunction<K, V> extends BaseRemappingFunction implements Function<Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>, Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>> {
//    private final Function<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> function;
//
//    BulkComputeMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> function) {
//      super(storeId, keyType, valueType);
//      this.function = function;
//    }
//
//    @Override
//    public Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> apply(Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> entries) {
//      Map<K, V> decodedEntries = new HashMap<>();
//      entries.forEach(entry -> {
//        K key = entry.getKey().getValue();
//        keyCheck(key);
//        CompositeValue<V> compositeValue = entry.getValue();
//        V value = null;
//        if (compositeValue != null) {
//          value = compositeValue.getValue();
//          valueCheck(value);
//        }
//        decodedEntries.put(key, value);
//      });
//      Map<CompositeValue<K>, CompositeValue<V>> encodedResults = new HashMap<>();
//      Iterable<Map.Entry<K, V>> results = function.apply(decodedEntries.entrySet());
//      results.forEach(entry -> {
//        keyCheck(entry.getKey());
//        valueCheck(entry.getValue());
//        encodedResults.put(new CompositeValue<>(storeId, entry.getKey()), new CompositeValue<>(storeId, entry.getValue()));
//      });
//      return encodedResults.entrySet();
//    }
//  }
//
//  public static class BulkComputeIfAbsentMappingFunction<K, V> extends BaseRemappingFunction implements Function<Iterable<CompositeValue<K>>, Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>>> {
//    private final Function<Iterable<K>, Iterable<Map.Entry<K, V>>> function;
//
//    BulkComputeIfAbsentMappingFunction(int storeId, Class<K> keyType, Class<V> valueType, Function<Iterable<K>, Iterable<Map.Entry<K, V>>> function) {
//      super(storeId, keyType, valueType);
//      this.function = function;
//    }
//
//    @Override
//    public Iterable<Map.Entry<CompositeValue<K>, CompositeValue<V>>> apply(Iterable<CompositeValue<K>> compositeValues) {
//      List<K> keys = new ArrayList<>();
//      compositeValues.forEach(k -> {
//        keyCheck(k.getValue());
//        keys.add(k.getValue());
//      });
//      Map<CompositeValue<K>, CompositeValue<V>> encodedResults = new HashMap<>();
//      Iterable<Map.Entry<K, V>> results = function.apply(keys);
//      results.forEach(entry -> {
//        keyCheck(entry.getKey());
//        valueCheck(entry.getValue());
//        encodedResults.put(new CompositeValue<>(storeId, entry.getKey()), new CompositeValue<>(storeId, entry.getValue()));
//      });
//      return encodedResults.entrySet();
//    }
//  }
//}
//
