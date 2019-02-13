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

package org.ehcache.impl.internal.store.disk;

import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.Status;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.impl.internal.events.ThreadLocalStoreEventDispatcher;
import org.ehcache.impl.internal.store.disk.factories.EhcachePersistentSegmentFactory;
import org.ehcache.impl.internal.store.offheap.AbstractOffHeapStore;
import org.ehcache.impl.internal.store.offheap.EhcacheOffHeapBackingMap;
import org.ehcache.impl.internal.store.offheap.OffHeapValueHolder;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.SerializerPortability;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.core.spi.service.LocalPersistenceService.PersistenceSpaceIdentifier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.SupplementaryService;
import org.ehcache.core.util.ConcurrentWeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.Persistent;
import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Long.max;
import static java.util.Arrays.asList;
import static org.ehcache.core.spi.ServiceLocator.findSingletonAmongst;
import static org.ehcache.core.spi.ServiceLocator.findSingletonAmongst;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

/**
 *
 * @author Chris Dennis
 */
public class OffHeapDiskStore<K, V> extends AbstractOffHeapStore<K, V> implements AuthoritativeTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapDiskStore.class);

  protected final AtomicReference<Status> status = new AtomicReference<Status>(Status.UNINITIALIZED);

  private final EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final long sizeInBytes;
  private final FileBasedPersistenceContext fileBasedPersistenceContext;
  private final ExecutionService executionService;
  private final String threadPoolAlias;
  private final int writerConcurrency;

  private volatile EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> map;

  public OffHeapDiskStore(FileBasedPersistenceContext fileBasedPersistenceContext,
                          ExecutionService executionService, String threadPoolAlias, int writerConcurrency,
                          final Configuration<K, V> config, TimeSource timeSource, StoreEventDispatcher<K, V> eventDispatcher, long sizeInBytes) {
    super("local-disk", config, timeSource, eventDispatcher);
    this.fileBasedPersistenceContext = fileBasedPersistenceContext;
    this.executionService = executionService;
    this.threadPoolAlias = threadPoolAlias;
    this.writerConcurrency = writerConcurrency;

    EvictionVeto<? super K, ? super V> veto = config.getEvictionVeto();
    if (veto != null) {
      evictionVeto = wrap(veto);
    } else {
      evictionVeto = Eviction.none();
    }
    this.keySerializer = config.getKeySerializer();
    this.valueSerializer = config.getValueSerializer();
    this.sizeInBytes = sizeInBytes;

    if (!status.compareAndSet(Status.UNINITIALIZED, Status.AVAILABLE)) {
      throw new AssertionError();
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> getBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto) {
    File dataFile = getDataFile();
    File indexFile = getIndexFile();

    if (dataFile.isFile() && indexFile.isFile()) {
      try {
        return recoverBackingMap(size, keySerializer, valueSerializer, evictionVeto);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
    }
  }

  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> recoverBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto) throws IOException {
    File dataFile = getDataFile();
    File indexFile = getIndexFile();

    FileInputStream fin = new FileInputStream(indexFile);
    try {
      ObjectInputStream input = new ObjectInputStream(fin);
      long dataTimestampFromIndex = input.readLong();
      long dataTimestampFromFile = dataFile.lastModified();
      long delta = dataTimestampFromFile - dataTimestampFromIndex;
      if (delta < 0) {
        LOGGER.info("The index for data file {} is more recent than the data file itself by {}ms : this is harmless.",
                    dataFile.getName(), -delta);
      } else if (delta > TimeUnit.SECONDS.toMillis(1)) {
        LOGGER.warn("The index for data file {} is out of date by {}ms, probably due to an unclean shutdown. Creating a new empty store.",
                    dataFile.getName(), delta);
        return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
      } else if (delta > 0) {
        LOGGER.info("The index for data file {} is out of date by {}ms, assuming this small delta is a result of the OS/filesystem.",
                    dataFile.getName(), delta);
      }

      MappedPageSource source = new MappedPageSource(dataFile, false, size);
      try {
        PersistentPortability<K> keyPortability = persistent(new SerializerPortability<K>(keySerializer));
        PersistentPortability<OffHeapValueHolder<V>> valuePortability = persistent(createValuePortability(valueSerializer));
        DiskWriteThreadPool writeWorkers = new DiskWriteThreadPool(executionService, threadPoolAlias, writerConcurrency);

        Factory<FileBackedStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = FileBackedStorageEngine.createFactory(source,
            max((size / 16) / 10, 1024), BYTES, keyPortability, valuePortability, writeWorkers, false);

        EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>> factory = new EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>>(
            source,
            storageEngineFactory,
            64,
            evictionVeto,
            mapEvictionListener, false);
            EhcachePersistentConcurrentOffHeapClockCache m = new EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(input, evictionVeto, factory);




        m.bootstrap(input);
        return m;
      } catch (IOException e) {
        source.close();
        throw e;
      }
    } catch (Exception e) {
      LOGGER.info("Index file was corrupt. Deleting data file " + dataFile.getAbsolutePath() +". " + e.getMessage());
      LOGGER.debug("Exception during recovery", e);
      return createBackingMap(size, keySerializer, valueSerializer, evictionVeto);
    } finally {
      fin.close();
    }
  }

  private EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> createBackingMap(long size, Serializer<K> keySerializer, Serializer<V> valueSerializer, EvictionVeto<K, OffHeapValueHolder<V>> evictionVeto) {
    MappedPageSource source;
    try {
      source = new MappedPageSource(getDataFile(), size);
    } catch (IOException e) {
      // TODO proper exception
      throw new RuntimeException(e);
    }
    PersistentPortability<K> keyPortability = persistent(new SerializerPortability<K>(keySerializer));
    PersistentPortability<OffHeapValueHolder<V>> valuePortability = persistent(createValuePortability(valueSerializer));
    DiskWriteThreadPool writeWorkers = new DiskWriteThreadPool(executionService, threadPoolAlias, writerConcurrency);

    Factory<FileBackedStorageEngine<K, OffHeapValueHolder<V>>> storageEngineFactory = FileBackedStorageEngine.createFactory(source,
        max((size / 16) / 10, 1024), BYTES, keyPortability, valuePortability, writeWorkers, true);

    EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>> factory = new EhcachePersistentSegmentFactory<K, OffHeapValueHolder<V>>(
        source,
        storageEngineFactory,
        64,
        evictionVeto,
        mapEvictionListener, true);
    return new EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>>(evictionVeto, factory, 16);

  }

  @Override
  protected EhcacheOffHeapBackingMap<K, OffHeapValueHolder<V>> backingMap() {
    return map;
  }

  private File getDataFile() {
    return new File(fileBasedPersistenceContext.getDirectory(), "ehcache-disk-store.data");
  }

  private File getIndexFile() {
    return new File(fileBasedPersistenceContext.getDirectory(), "ehcache-disk-store.index");
  }

  @SupplementaryService
  @ServiceDependencies({TimeSourceService.class, SerializationProvider.class, ExecutionService.class})
  public static class Provider implements Store.Provider, AuthoritativeTier.Provider {

    private final Set<Store<?, ?>> createdStores = Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<Store<?, ?>, Boolean>());
    private final String defaultThreadPool;
    private volatile ServiceProvider<Service> serviceProvider;

    public Provider() {
      this(null);
    }

    public Provider(String threadPoolAlias) {
      this.defaultThreadPool = threadPoolAlias;
    }

    @Override
    public <K, V> OffHeapDiskStore<K, V> createStore(Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return createStoreInternal(storeConfig, new ThreadLocalStoreEventDispatcher<K, V>(storeConfig.getOrderedEventParallelism()), serviceConfigs);
    }

    private <K, V> OffHeapDiskStore<K, V> createStoreInternal(Configuration<K, V> storeConfig, StoreEventDispatcher<K, V> eventDispatcher, ServiceConfiguration<?>... serviceConfigs) {
      if (serviceProvider == null) {
        throw new NullPointerException("ServiceProvider is null in OffHeapDiskStore.Provider.");
      }
      TimeSource timeSource = serviceProvider.getService(TimeSourceService.class).getTimeSource();
      ExecutionService executionService = serviceProvider.getService(ExecutionService.class);

      ResourcePool diskPool = storeConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK);
      if (!(diskPool.getUnit() instanceof MemoryUnit)) {
        throw new IllegalArgumentException("OffHeapDiskStore only supports resources configuration expressed in \"memory\" unit");
      }
      MemoryUnit unit = (MemoryUnit)diskPool.getUnit();

      LocalPersistenceService localPersistenceService = serviceProvider.getService(LocalPersistenceService.class);
      if (localPersistenceService == null) {
        throw new IllegalStateException("No LocalPersistenceService could be found - did you configure it at the CacheManager level?");
      }

      String threadPoolAlias;
      int writerConcurrency;
      OffHeapDiskStoreConfiguration config = findSingletonAmongst(OffHeapDiskStoreConfiguration.class, serviceConfigs);
      if (config == null) {
        threadPoolAlias = defaultThreadPool;
        writerConcurrency = 1;
      } else {
        threadPoolAlias = config.getThreadPoolAlias();
        writerConcurrency = config.getWriterConcurrency();
      }
      PersistenceSpaceIdentifier space = findSingletonAmongst(PersistenceSpaceIdentifier.class, (Object[]) serviceConfigs);
      try {
        FileBasedPersistenceContext persistenceContext = localPersistenceService.createPersistenceContextWithin(space , "offheap-disk-store");

        OffHeapDiskStore<K, V> offHeapStore = new OffHeapDiskStore<K, V>(persistenceContext,
                executionService, threadPoolAlias, writerConcurrency,
                storeConfig, timeSource, eventDispatcher, unit.toBytes(diskPool.getSize()));
        createdStores.add(offHeapStore);
        return offHeapStore;
      } catch (CachePersistenceException cpex) {
        throw new RuntimeException("Unable to create persistence context in " + space, cpex);
      }
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      try {
        close((OffHeapDiskStore)resource);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    static <K, V> void close(final OffHeapDiskStore<K, V> resource) throws IOException {
      EhcachePersistentConcurrentOffHeapClockCache<K, OffHeapValueHolder<V>> localMap = resource.map;
      if (localMap != null) {
        resource.map = null;
        localMap.flush();
        ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(resource.getIndexFile()));
        try {
          output.writeLong(System.currentTimeMillis());
          localMap.persist(output);
        } finally {
          output.close();
        }
        localMap.close();
      }
    }

    @Override
    public void initStore(Store<?, ?> resource) {
      if (!createdStores.contains(resource)) {
        throw new IllegalArgumentException("Given store is not managed by this provider : " + resource);
      }
      init((OffHeapDiskStore)resource);
    }

    static <K, V> void init(final OffHeapDiskStore<K, V> resource) {
      resource.map = resource.getBackingMap(resource.sizeInBytes, resource.keySerializer, resource.valueSerializer, resource.evictionVeto);
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
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

  /*
   * This is kind of a hack, but it's safe to use this if the regular portability
   * is stateless.
   */
  @SuppressWarnings("unchecked")
  public static <T> PersistentPortability<T> persistent(final Portability<T> normal) {
    if (normal instanceof PersistentPortability<?>) {
      return (PersistentPortability<T>) normal;
    } else {
      LinkedHashSet<Class<?>> proxyInterfaces = new LinkedHashSet<>();
      for (Class<?> klazz = normal.getClass(); klazz != null; klazz = klazz.getSuperclass()) {
        proxyInterfaces.addAll(asList(klazz.getInterfaces()));
      }
      proxyInterfaces.add(PersistentPortability.class);

      return (PersistentPortability<T>) Proxy.newProxyInstance(normal.getClass().getClassLoader(), proxyInterfaces.toArray(new Class[0]), (o, method, os) -> {
        if (method.getDeclaringClass().equals(Persistent.class)) {
          return null;
        } else {
          return method.invoke(normal, os);
        }
      });
    }
  }
}
