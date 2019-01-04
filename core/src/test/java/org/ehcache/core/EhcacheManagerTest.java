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

package org.ehcache.core;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.Status;
import org.ehcache.UserManagedCache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.events.CacheEventListenerProvider;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceProvider;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class EhcacheManagerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static Map<String, CacheConfiguration<?, ?>> newCacheMap() {
    return new HashMap<>();
  }

  private List<Service> minimumCacheManagerServices() {
    return new ArrayList<>(Arrays.asList(
      mock(Store.Provider.class),
      mock(CacheLoaderWriterProvider.class),
      mock(WriteBehindProvider.class),
      mock(CacheEventDispatcherFactory.class),
      mock(CacheEventListenerProvider.class),
      mock(LocalPersistenceService.class),
      mock(ResilienceStrategyProvider.class)));
  }

  @Test
  public void testCanDestroyAndClose() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = new BaseCacheConfiguration<>(Long.class, String.class, null,
      null, null, ResourcePoolsHelper.createHeapOnlyPools(10));

    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    Store store = mock(Store.class);
    CacheEventDispatcherFactory cacheEventNotificationListenerServiceProvider = mock(CacheEventDispatcherFactory.class);

    when(storeProvider.createStore(any(Store.Configuration.class), ArgumentMatchers.<ServiceConfiguration>any())).thenReturn(store);
    when(store.getConfigurationChangeListeners()).thenReturn(new ArrayList<>());
    when(cacheEventNotificationListenerServiceProvider.createCacheEventDispatcher(store)).thenReturn(mock(CacheEventDispatcher.class));

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("aCache", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    PersistentCacheManager cacheManager = new EhcacheManager(config, Arrays.asList(
        storeProvider,
        mock(CacheLoaderWriterProvider.class),
        mock(WriteBehindProvider.class),
        cacheEventNotificationListenerServiceProvider,
        mock(CacheEventListenerProvider.class),
        mock(LocalPersistenceService.class),
        mock(ResilienceStrategyProvider.class)));
    cacheManager.init();

    cacheManager.close();
    cacheManager.init();
    cacheManager.close();
    cacheManager.destroy();
    cacheManager.init();
    cacheManager.close();
  }

  @Test
  public void testConstructionThrowsWhenNotBeingToResolveService() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    final DefaultConfiguration config = new DefaultConfiguration(caches, null, (ServiceCreationConfiguration<NoSuchService>) () -> NoSuchService.class);
    try {
      new EhcacheManager(config);
      fail("Should have thrown...");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString(NoSuchService.class.getName()));
    }
  }

  @Test
  public void testCreationFailsOnDuplicateServiceCreationConfiguration() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null, (ServiceCreationConfiguration<NoSuchService>) () -> NoSuchService.class, (ServiceCreationConfiguration<NoSuchService>) () -> NoSuchService.class);
    try {
      new EhcacheManager(config);
      fail("Should have thrown ...");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("NoSuchService"));
    }
  }

  @Test
  public void testStopAllServicesWhenCacheInitializationFails() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("myCache", mock(CacheConfiguration.class));
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    List<Service> services = minimumCacheManagerServices();
    EhcacheManager cacheManager = new EhcacheManager(config, services);

    Store.Provider storeProvider = (Store.Provider) services.get(0); // because I know it's the first of the list

    try {
      cacheManager.init();
      fail("Should have thrown...");
    } catch (StateTransitionException ste) {
      verify(storeProvider).stop();
    }
  }

  @Test
  public void testNoClassLoaderSpecified() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("foo", new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper.createHeapOnlyPools()));
    DefaultConfiguration config = new DefaultConfiguration(caches, null);

    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Store mock = mock(Store.class);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    cacheManager.init();
    assertSame(ClassLoading.getDefaultClassLoader(), cacheManager.getClassLoader());
    assertSame(cacheManager.getClassLoader(), cacheManager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
  }

  @Test
  public void testClassLoaderSpecified() {
    ClassLoader cl1 = new ClassLoader() {
      //
    };

    ClassLoader cl2 = new ClassLoader() {
      //
    };

    assertNotSame(cl1, cl2);
    assertNotSame(cl1.getClass(), cl2.getClass());

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("foo1", new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper.createHeapOnlyPools()));
    caches.put("foo2", new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper.createHeapOnlyPools()));
    caches.put("foo3", new BaseCacheConfiguration<>(Object.class, Object.class, null, cl2, null, ResourcePoolsHelper.createHeapOnlyPools()));
    DefaultConfiguration config = new DefaultConfiguration(caches, cl1);

    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Store mock = mock(Store.class);
    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    cacheManager.init();
    assertSame(cl1, cacheManager.getClassLoader());
    assertSame(cl1, cacheManager.getCache("foo1", Object.class, Object.class)
        .getRuntimeConfiguration()
        .getClassLoader());
    assertSame(cl1, cacheManager.getCache("foo2", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
    assertSame(cl2, cacheManager.getCache("foo3", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
  }


  @Test
  public void testReturnsNullForNonExistCache() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, getServices(null, null));
    cacheManager.init();
    assertThat(cacheManager.getCache("foo", Object.class, Object.class), nullValue());
  }

  @Test
  public void testThrowsWhenAddingExistingCache() {
    CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Store mock = mock(Store.class);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(storeProvider, cenlProvider);

    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("bar", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);

    EhcacheManager cacheManager = new EhcacheManager(config, services);
    cacheManager.init();
    final Cache<Object, Object> cache = cacheManager.getCache("bar", Object.class, Object.class);
    assertNotNull(cache);
    try {
      cacheManager.createCache("bar", cacheConfiguration);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("bar"));
    }
  }

  @Test
  public void testThrowsWhenNotInitialized() {
    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Store mock = mock(Store.class);
    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(storeProvider, cenlProvider);

    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);

    final CacheConfiguration<Integer, String> cacheConfiguration = new BaseCacheConfiguration<>(Integer.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("bar", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    try {
      cacheManager.removeCache("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      cacheManager.createCache("foo", (CacheConfiguration) null);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      cacheManager.getCache("foo", Object.class, Object.class);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
  }
  @Test
  public void testThrowsWhenRetrievingCacheWithWrongTypes() {
    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Store mock = mock(Store.class);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);

    final CacheConfiguration<Integer, String> cacheConfiguration = new BaseCacheConfiguration<>(Integer.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("bar", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    cacheManager.init();
    cacheManager.getCache("bar", Integer.class, String.class);
    try {
      cacheManager.getCache("bar", Integer.class, Integer.class);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("bar"));
      assertTrue(e.getMessage().contains("<java.lang.Integer, java.lang.String>"));
      assertTrue(e.getMessage().contains("<java.lang.Integer, java.lang.Integer>"));
    }
    try {
      cacheManager.getCache("bar", String.class, String.class);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("bar"));
      assertTrue(e.getMessage().contains("<java.lang.Integer, java.lang.String>"));
      assertTrue(e.getMessage().contains("<java.lang.String, java.lang.String>"));
    }
  }

  @Ignore
  @Test
  public void testLifeCyclesCacheLoaders() throws Exception {

    ResourcePools resourcePools = ResourcePoolsHelper.createHeapOnlyPools(10);

    final CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);

    final CacheConfiguration<Long, Long> barConfig = mock(CacheConfiguration.class);
    when(barConfig.getClassLoader()).thenReturn(getClass().getClassLoader());
    when(barConfig.getResourcePools()).thenReturn(resourcePools);
    final CacheConfiguration<Integer, CharSequence> fooConfig = mock(CacheConfiguration.class);
    when(fooConfig.getClassLoader()).thenReturn(getClass().getClassLoader());
    when(fooConfig.getResourcePools()).thenReturn(resourcePools);

    CacheLoaderWriter fooLoaderWriter = mock(CacheLoaderWriter.class);

    final WriteBehindProvider decoratorLoaderWriterProvider = mock(WriteBehindProvider.class);

    when(cacheLoaderWriterProvider.createCacheLoaderWriter("foo", fooConfig)).thenReturn(fooLoaderWriter);

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("bar", barConfig);
    caches.put("foo", fooConfig);

    Configuration cfg = new DefaultConfiguration(
        caches,
        getClass().getClassLoader()
    );

    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    Store mock = mock(Store.class);
    CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mock)).thenReturn(cenlServiceMock);
    Collection<Service> services = getServices(cacheLoaderWriterProvider, decoratorLoaderWriterProvider, storeProvider, cenlProvider);
    when(storeProvider
        .createStore(ArgumentMatchers.<Store.Configuration>any(), ArgumentMatchers.<ServiceConfiguration[]>any())).thenReturn(mock);

    EhcacheManager manager = new EhcacheManager(cfg, services);
    manager.init();

    verify(cacheLoaderWriterProvider).createCacheLoaderWriter("bar", barConfig);
    verify(cacheLoaderWriterProvider).createCacheLoaderWriter("foo", fooConfig);

    manager.removeCache("bar");
    verify(cacheLoaderWriterProvider, never()).releaseCacheLoaderWriter(anyString(), (CacheLoaderWriter<?, ?>)Mockito.any());
    manager.removeCache("foo");
    verify(cacheLoaderWriterProvider).releaseCacheLoaderWriter(anyString(), fooLoaderWriter);
  }

  @Test
  public void testDoesNotifyAboutCache() {
    final CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider mock = mock(Store.Provider.class);
    when(mock.rank(any(Set.class), any(Collection.class))).thenReturn(1);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(any(Store.class))).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(mock, cenlProvider);
    when(mock.createStore(ArgumentMatchers.<Store.Configuration>any())).thenReturn(mock(Store.class));
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    final CacheManagerListener listener = mock(CacheManagerListener.class);
    cacheManager.registerListener(listener);
    cacheManager.init();
    final String cacheAlias = "bar";
    cacheManager.createCache(cacheAlias, cacheConfiguration);
    final Cache<Object, Object> bar = cacheManager.getCache(cacheAlias, Object.class, Object.class);
    verify(listener).cacheAdded(cacheAlias, bar);
    cacheManager.removeCache(cacheAlias);
    verify(listener).cacheRemoved(cacheAlias, bar);
  }

  @Test
  public void testDoesNotNotifyAboutCacheOnInitOrClose() {
    final CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider mock = mock(Store.Provider.class);
    when(mock.rank(any(Set.class), any(Collection.class))).thenReturn(1);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(any(Store.class))).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(mock, cenlProvider);
    when(mock.createStore(ArgumentMatchers.<Store.Configuration>any())).thenReturn(mock(Store.class));
    final String cacheAlias = "bar";
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put(cacheAlias, cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services);
    final CacheManagerListener listener = mock(CacheManagerListener.class);
    cacheManager.registerListener(listener);
    cacheManager.init();
    final Cache<Object, Object> bar = cacheManager.getCache(cacheAlias, Object.class, Object.class);
    verify(listener, never()).cacheAdded(cacheAlias, bar);
    cacheManager.close();
    verify(listener, never()).cacheRemoved(cacheAlias, bar);
  }

  @Test
  public void testClosesStartedCachesDownWhenInitThrows() {
    final Set<Cache<?,?>> caches = new HashSet<>();
    final CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    final Collection<Service> services = getServices(storeProvider, null);
    final RuntimeException thrown = new RuntimeException();
    when(storeProvider.createStore(ArgumentMatchers.<Store.Configuration>any())).thenReturn(mock(Store.class));
    Map<String, CacheConfiguration<?, ?>> cacheMap = newCacheMap();
    cacheMap.put("foo", cacheConfiguration);
    cacheMap.put("bar", cacheConfiguration);
    cacheMap.put("foobar", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(cacheMap, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services) {

      @Override
      <K, V> InternalCache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                            final Class<K> keyType, final Class<V> valueType) {
        final InternalCache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        caches.add(ehcache);
        if(caches.size() == 1) {
          when(storeProvider.createStore(
                  ArgumentMatchers.<Store.Configuration<K,V>>any(), ArgumentMatchers.<ServiceConfiguration<?>>any()))
              .thenThrow(thrown);
        }
        return ehcache;
      }

      @Override
      protected void closeEhcache(final String alias, final InternalCache<?, ?> ehcache) {
        super.closeEhcache(alias, ehcache);
        caches.remove(ehcache);
      }
    };

    try {
      cacheManager.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(cacheManager.getStatus(), is(Status.UNINITIALIZED));
      final String message = e.getCause().getMessage();
      assertThat(message, CoreMatchers.startsWith("Cache '"));
      assertThat(message, containsString("' creation in "));
      assertThat(message, CoreMatchers.endsWith(" failed."));
    }
    assertThat(caches.isEmpty(), is(true));
  }

  @Test
  public void testClosesAllCachesDownWhenCloseThrows() {
    final Set<String> caches = new HashSet<>();
    final CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);

    final CacheEventDispatcherFactory cenlProvider = mock(CacheEventDispatcherFactory.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(any(Store.class))).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    final RuntimeException thrown = new RuntimeException();
    when(storeProvider.createStore(ArgumentMatchers.<Store.Configuration>any())).thenReturn(mock(Store.class));
    Map<String, CacheConfiguration<?, ?>> cacheMap = newCacheMap();
    cacheMap.put("foo", cacheConfiguration);
    cacheMap.put("bar", cacheConfiguration);
    cacheMap.put("foobar", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(cacheMap, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services) {

      @Override
      <K, V> InternalCache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                            final Class<K> keyType, final Class<V> valueType) {
        final InternalCache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        caches.add(alias);
        return ehcache;
      }

      @Override
      protected void closeEhcache(final String alias, final InternalCache<?, ?> ehcache) {
        super.closeEhcache(alias, ehcache);
        if(alias.equals("foobar")) {
          throw thrown;
        }
        caches.remove(alias);
      }
    };

    cacheManager.init();
    try {
      cacheManager.close();
      fail();
    } catch (StateTransitionException e) {
      assertThat(cacheManager.getStatus(), is(Status.UNINITIALIZED));
      assertThat(e.getCause(), CoreMatchers.<Throwable>sameInstance(thrown));
    }
    assertThat(caches.contains("foobar"), is(true));
  }

  @Test
  public void testDoesNotifyAboutLifecycle() {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, getServices(null, null));
    final CacheManagerListener listener = mock(CacheManagerListener.class);
    cacheManager.registerListener(listener);
    cacheManager.init();
    verify(listener).stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    cacheManager.close();
    verify(listener).stateTransition(Status.AVAILABLE, Status.UNINITIALIZED);
  }

  @Test
  public void testCloseNoLoaderWriterAndCacheEventListener() throws Exception {
    final CacheConfiguration<Object, Object> cacheConfiguration = new BaseCacheConfiguration<>(Object.class, Object.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    final Store.Provider storeProvider = spy(new Store.Provider() {
      @Override
      public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
        return 1;
      }

      @Override
      public void stop() {
      }

      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
      }

      @Override
      public void releaseStore(Store<?, ?> resource) {

      }

      @Override
      public void initStore(Store<?, ?> resource) {

      }

      @Override
      public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
        return null;
      }
    });

    final CacheEventDispatcherFactory cenlProvider = spy(new CacheEventDispatcherFactory() {
      @Override
      public void start(ServiceProvider<Service> serviceProvider) {
      }

      @Override
      public void stop() {
      }

      @Override
      public <K, V> CacheEventDispatcher<K, V> createCacheEventDispatcher(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs) {
        return null;
      }

      @Override
      public <K, V> void releaseCacheEventDispatcher(CacheEventDispatcher<K, V> eventDispatcher) {
        eventDispatcher.shutdown();
      }
    });
    Store mockStore = mock(Store.class);
    final CacheEventDispatcher<Object, Object> cenlServiceMock = mock(CacheEventDispatcher.class);
    when(cenlProvider.createCacheEventDispatcher(mockStore)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    List<CacheConfigurationChangeListener> configurationChangeListenerList = new ArrayList<>();
    configurationChangeListenerList.add(mock(CacheConfigurationChangeListener.class));
    when(mockStore.getConfigurationChangeListeners()).thenReturn(configurationChangeListenerList);
    when(storeProvider.createStore(ArgumentMatchers.<Store.Configuration>any())).thenReturn(mockStore);

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("foo", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    EhcacheManager cacheManager = new EhcacheManager(config, services) {
      @Override
      <K, V> InternalCache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config, final Class<K> keyType, final Class<V> valueType) {
        final InternalCache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        return spy(ehcache);
      }
    };
    cacheManager.init();
    Cache<Object, Object> testCache = cacheManager.getCache("foo", Object.class, Object.class);
    cacheManager.close();
    verify((UserManagedCache)testCache).close();
    verify(cenlServiceMock, times(1)).shutdown();
  }

  @Test
  public void testChangesToManagerAreReflectedInConfig() {
    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    Store store = mock(Store.class);
    CacheEventDispatcherFactory cacheEventNotificationListenerServiceProvider = mock(CacheEventDispatcherFactory.class);

    when(storeProvider.createStore(any(Store.Configuration.class), ArgumentMatchers.<ServiceConfiguration>any())).thenReturn(store);
    when(store.getConfigurationChangeListeners()).thenReturn(new ArrayList<>());
    when(cacheEventNotificationListenerServiceProvider.createCacheEventDispatcher(store)).thenReturn(mock(CacheEventDispatcher.class));

    CacheConfiguration<Long, String> cache1Configuration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("cache1", cache1Configuration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);

    CacheManager cacheManager = new EhcacheManager(config, Arrays.asList(storeProvider,
        mock(CacheLoaderWriterProvider.class),
        mock(WriteBehindProvider.class),
        cacheEventNotificationListenerServiceProvider,
        mock(CacheEventListenerProvider.class),
        mock(LocalPersistenceService.class),
        mock(ResilienceStrategyProvider.class)
    ));
    cacheManager.init();

    try {
      final CacheConfiguration<Long, String> cache2Configuration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
        .createHeapOnlyPools());
      final Cache<Long, String> cache = cacheManager.createCache("cache2", cache2Configuration);
      final CacheConfiguration<?, ?> cacheConfiguration = cacheManager.getRuntimeConfiguration()
          .getCacheConfigurations()
          .get("cache2");

      assertThat(cacheConfiguration, notNullValue());
      final CacheConfiguration<?, ?> runtimeConfiguration = cache.getRuntimeConfiguration();
      assertThat(cacheConfiguration == runtimeConfiguration, is(true));
      assertThat(cacheManager.getRuntimeConfiguration().getCacheConfigurations().get("cache1")
                 == cacheManager.getCache("cache1", Long.class, String.class).getRuntimeConfiguration(), is(true));

      cacheManager.removeCache("cache1");
      assertThat(cacheManager.getRuntimeConfiguration().getCacheConfigurations().containsKey("cache1"), is(false));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testCachesAddedAtRuntimeGetReInited() {
    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    Store store = mock(Store.class);
    CacheEventDispatcherFactory cacheEventNotificationListenerServiceProvider = mock(CacheEventDispatcherFactory.class);

    when(storeProvider.createStore(any(Store.Configuration.class), ArgumentMatchers.<ServiceConfiguration>any())).thenReturn(store);
    when(store.getConfigurationChangeListeners()).thenReturn(new ArrayList<>());
    when(cacheEventNotificationListenerServiceProvider.createCacheEventDispatcher(store)).thenReturn(mock(CacheEventDispatcher.class));

    CacheConfiguration<Long, String> cache1Configuration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("cache1", cache1Configuration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    CacheManager cacheManager = new EhcacheManager(config, Arrays.asList(
        storeProvider,
        mock(CacheLoaderWriterProvider.class),
        mock(WriteBehindProvider.class),
        cacheEventNotificationListenerServiceProvider,
        mock(CacheEventListenerProvider.class),
        mock(LocalPersistenceService.class),
        mock(ResilienceStrategyProvider.class)
    ));
    cacheManager.init();


    CacheConfiguration<Long, String> cache2Configuration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    cacheManager.createCache("cache2", cache2Configuration);
    cacheManager.removeCache("cache1");

    cacheManager.close();
    cacheManager.init();
    try {
      assertThat(cacheManager.getCache("cache1", Long.class, String.class), nullValue());
      assertThat(cacheManager.getCache("cache2", Long.class, String.class), notNullValue());
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testCloseWhenRuntimeCacheCreationFails() throws Exception {
    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    doThrow(new Error("Test EhcacheManager close.")).when(storeProvider).createStore(any(Store.Configuration.class), ArgumentMatchers.<ServiceConfiguration>any());

    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    final CacheManager cacheManager = new EhcacheManager(config, Arrays.asList(
        storeProvider,
        mock(CacheLoaderWriterProvider.class),
        mock(WriteBehindProvider.class),
        mock(CacheEventDispatcherFactory.class),
        mock(CacheEventListenerProvider.class),
        mock(LocalPersistenceService.class),
        mock(ResilienceStrategyProvider.class)
    ));

    cacheManager.init();

    CacheConfiguration<Long, String> cacheConfiguration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());

    try {
      cacheManager.createCache("cache", cacheConfiguration);
      fail();
    } catch (Error err) {
      assertThat(err.getMessage(), equalTo("Test EhcacheManager close."));
    }

    cacheManager.close();
    assertThat(cacheManager.getStatus(), is(Status.UNINITIALIZED));

  }

  @Test(timeout = 2000L)
  public void testCloseWhenCacheCreationFailsDuringInitialization() throws Exception {
    Store.Provider storeProvider = mock(Store.Provider.class);
    when(storeProvider.rank(any(Set.class), any(Collection.class))).thenReturn(1);
    doThrow(new Error("Test EhcacheManager close.")).when(storeProvider).createStore(any(Store.Configuration.class), ArgumentMatchers.<ServiceConfiguration>any());

    CacheConfiguration<Long, String> cacheConfiguration = new BaseCacheConfiguration<>(Long.class, String.class, null, null, null, ResourcePoolsHelper
      .createHeapOnlyPools());
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    caches.put("cache1", cacheConfiguration);
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    final CacheManager cacheManager = new EhcacheManager(config, Arrays.asList(
        storeProvider,
        mock(CacheLoaderWriterProvider.class),
        mock(WriteBehindProvider.class),
        mock(CacheEventDispatcherFactory.class),
        mock(CacheEventListenerProvider.class),
        mock(LocalPersistenceService.class),
        mock(ResilienceStrategyProvider.class)
    ));

    final CountDownLatch countDownLatch = new CountDownLatch(1);

    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        cacheManager.init();
      } catch (Error err) {
        assertThat(err.getMessage(), equalTo("Test EhcacheManager close."));
        countDownLatch.countDown();
      }
    });
    countDownLatch.await();
    try {
      cacheManager.close();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Close not supported from UNINITIALIZED"));
    }
    assertThat(cacheManager.getStatus(), is(Status.UNINITIALIZED));

  }

  @Test
  public void testDestroyCacheFailsIfAlreadyInMaintenanceMode() throws CachePersistenceException, InterruptedException {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    final EhcacheManager manager = new EhcacheManager(config, minimumCacheManagerServices());

    Thread thread = new Thread(() -> manager.getStatusTransitioner().maintenance().succeeded());
    thread.start();
    thread.join(1000);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("State is MAINTENANCE, yet you don't own it!");

    manager.destroyCache("test");
  }

  @Test
  public void testDestroyCacheFailsAndStopIfStartingServicesFails() throws CachePersistenceException, InterruptedException {
    Map<String, CacheConfiguration<?, ?>> caches = newCacheMap();
    DefaultConfiguration config = new DefaultConfiguration(caches, null);
    List<Service> services = minimumCacheManagerServices();
    MaintainableService service = mock(MaintainableService.class);
    doThrow(new RuntimeException("failed")).when(service)
      .startForMaintenance(Mockito.<ServiceProvider<MaintainableService>>any(), eq(MaintainableService.MaintenanceScope.CACHE));
    services.add(service);

    EhcacheManager manager = new EhcacheManager(config, services);

    expectedException.expect(StateTransitionException.class);
    expectedException.expectMessage("failed");

    manager.destroyCache("test");

    assertThat(manager.getStatus(), equalTo(Status.UNINITIALIZED));
  }

  private Collection<Service> getServices(Store.Provider storeProvider, CacheEventDispatcherFactory cenlProvider) {
    return getServices(mock(CacheLoaderWriterProvider.class), mock(WriteBehindProvider.class),
        storeProvider != null ? storeProvider : mock(Store.Provider.class),
        cenlProvider != null ? cenlProvider : mock(CacheEventDispatcherFactory.class));
  }

  private Collection<Service> getServices(CacheLoaderWriterProvider cacheLoaderWriterProvider,
                                     WriteBehindProvider decoratorLoaderWriterProvider,
                                     Store.Provider storeProvider,
                                     CacheEventDispatcherFactory cenlProvider) {
    return new ArrayList<>(Arrays.asList(cacheLoaderWriterProvider, storeProvider, decoratorLoaderWriterProvider, cenlProvider, mock(CacheEventListenerProvider.class), mock(ResilienceStrategyProvider.class)));
  }

  static class NoSuchService implements Service {

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

}
