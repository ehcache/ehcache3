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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.DefaultConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.events.CacheEventNotificationListenerServiceProvider;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.CacheEventNotificationServiceImpl;
import org.ehcache.events.CacheManagerListener;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.config.ConfigurationBuilder;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindDecoratorLoaderWriterProvider;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.ehcache.util.ClassLoading;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.ConfigurationBuilder.newConfigurationBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class EhcacheManagerTest {

  @Test
  public void testCanGoInMaintenanceAndClose() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    Store.Provider storeProvider = mock(Store.Provider.class);
    Store store = mock(Store.class);
    CacheEventNotificationListenerServiceProvider cacheEventNotificationListenerServiceProvider = mock(CacheEventNotificationListenerServiceProvider.class);

    when(storeProvider.createStore(any(Store.Configuration.class), Matchers.<ServiceConfiguration>anyVararg())).thenReturn(store);
    when(store.getConfigurationChangeListeners()).thenReturn(new ArrayList<CacheConfigurationChangeListener>());
    when(cacheEventNotificationListenerServiceProvider.createCacheEventNotificationService(store)).thenReturn(mock(CacheEventNotificationService.class));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(storeProvider)
        .using(mock(CacheLoaderWriterProvider.class))
        .using(mock(WriteBehindDecoratorLoaderWriterProvider.class))
        .using(cacheEventNotificationListenerServiceProvider)
        .using(mock(CacheEventListenerProvider.class))
        .using(mock(LocalPersistenceService.class))
        .build(true);

    cacheManager.close();
    cacheManager.init();
    cacheManager.close();
    Maintainable maintainable = ((PersistentCacheManager) cacheManager).toMaintenance();
    maintainable.close();
    cacheManager.init();
    cacheManager.close();
  }

  @Test
  public void testInitThrowsWhenNotBeingToResolveService() {
    final Configuration config = newConfigurationBuilder().addService(new ServiceCreationConfiguration<NoSuchService>() {
      @Override
      public Class<NoSuchService> getServiceType() {
        return NoSuchService.class;
      }
    }).build();
    final EhcacheManager ehcacheManager = new EhcacheManager(config);
    try {
      ehcacheManager.init();
      fail("Should have thrown...");
    } catch (StateTransitionException e) {
      assertTrue(e.getMessage().contains(NoSuchService.class.getName()));
      assertTrue(e.getCause().getMessage().contains(NoSuchService.class.getName()));
    }
  }
  
  @Test
  public void testNoClassLoaderSpecified() {
    ConfigurationBuilder builder = newConfigurationBuilder().addCache("foo", newCacheConfigurationBuilder().buildConfig(Object.class, Object.class));
    final Store.Provider storeProvider = mock(Store.Provider.class);
    final Store mock = mock(Store.class);
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mock)).thenReturn(cenlServiceMock);
    
    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);
    EhcacheManager cacheManager = new EhcacheManager(builder.build(), services);
    cacheManager.init();
    assertSame(ClassLoading.getDefaultClassLoader(), cacheManager.getClassLoader());
    assertSame(cacheManager.getClassLoader(), cacheManager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
    
    // explicit null
    builder = newConfigurationBuilder().withClassLoader(null).addCache("foo", newCacheConfigurationBuilder().buildConfig(Object.class, Object.class));
    cacheManager = new EhcacheManager(builder.build(), getServices(storeProvider, cenlProvider));
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
    
    ConfigurationBuilder builder = newConfigurationBuilder().withClassLoader(cl1)
            // these caches should inherit the cache manager classloader
            .addCache("foo1", newCacheConfigurationBuilder().buildConfig(Object.class, Object.class))
            .addCache("foo2", newCacheConfigurationBuilder().withClassLoader(null).buildConfig(Object.class, Object.class))
            // this cache specifies its own unique classloader
            .addCache("foo3", newCacheConfigurationBuilder().withClassLoader(cl2).buildConfig(Object.class, Object.class));

    final Store.Provider storeProvider = mock(Store.Provider.class);
    final Store mock = mock(Store.class);
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mock)).thenReturn(cenlServiceMock);

    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);
    EhcacheManager cacheManager = new EhcacheManager(builder.build(), services);
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
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().build(), getServices(null, null));
    cacheManager.init();
    assertThat(cacheManager.getCache("foo", Object.class, Object.class), nullValue());
  }

  @Test
  public void testThrowsWhenAddingExistingCache() {
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider storeProvider = mock(Store.Provider.class);
    final Store mock = mock(Store.class);

    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mock)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(storeProvider, cenlProvider);

    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);

    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("bar",
        cacheConfiguration)
        .build(), services);
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
    final Store mock = mock(Store.class);
    final Collection<Service> services = new ArrayList<Service>();
    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);

    final CacheConfiguration<Integer, String> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Integer.class, String.class);
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("bar",
        cacheConfiguration)
        .build(), services);
    try {
      cacheManager.removeCache("foo");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains(Status.UNINITIALIZED.name()), is(true));
    }
    try {
      cacheManager.createCache("foo", null);
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
    final Store mock = mock(Store.class);
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mock)).thenReturn(cenlServiceMock);
    
    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);

    final CacheConfiguration<Integer, String> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Integer.class, String.class);
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("bar",
        cacheConfiguration)
        .build(), services);
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

  @Test
  public void testLifeCyclesCacheLoaders() {

    ResourcePools resourcePools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10L, EntryUnit.ENTRIES).build();

    final CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);

    final CacheConfiguration<Long, Long> barConfig = mock(CacheConfiguration.class);
    when(barConfig.getClassLoader()).thenReturn(getClass().getClassLoader());
    when(barConfig.getResourcePools()).thenReturn(resourcePools);
    final CacheConfiguration<Integer, CharSequence> fooConfig = mock(CacheConfiguration.class);
    when(fooConfig.getClassLoader()).thenReturn(getClass().getClassLoader());
    when(fooConfig.getResourcePools()).thenReturn(resourcePools);

    CacheLoaderWriter fooLoaderWriter = mock(CacheLoaderWriter.class);
    
    final WriteBehindDecoratorLoaderWriterProvider decoratorLoaderWriterProvider = mock(WriteBehindDecoratorLoaderWriterProvider.class);

    when(cacheLoaderWriterProvider.createCacheLoaderWriter("foo", fooConfig)).thenReturn(fooLoaderWriter);
    

    @SuppressWarnings("serial")
    final Configuration cfg = new DefaultConfiguration(
        new HashMap<String, CacheConfiguration<?, ?>>() {{
          put("bar", barConfig);
          put("foo", fooConfig);
        }},
        getClass().getClassLoader()
    );

    final Store.Provider storeProvider = mock(Store.Provider.class);
    final Store mock = mock(Store.class);
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mock)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(cacheLoaderWriterProvider, decoratorLoaderWriterProvider, storeProvider, cenlProvider);
    when(storeProvider
        .createStore(Matchers.<Store.Configuration>anyObject(), Matchers.<ServiceConfiguration[]>anyVararg())).thenReturn(mock);

    final EhcacheManager manager = new EhcacheManager(cfg, services);
    manager.init();

    verify(cacheLoaderWriterProvider).createCacheLoaderWriter("bar", barConfig);
    verify(cacheLoaderWriterProvider).createCacheLoaderWriter("foo", fooConfig);

    manager.removeCache("bar");
    verify(cacheLoaderWriterProvider, never()).releaseCacheLoaderWriter((CacheLoaderWriter<?, ?>)Mockito.anyObject());
    manager.removeCache("foo");
    verify(cacheLoaderWriterProvider).releaseCacheLoaderWriter(fooLoaderWriter);
  }

  @Test
  public void testDoesNotifyAboutCache() {
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider mock = mock(Store.Provider.class);
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(any(Store.class))).thenReturn(cenlServiceMock);
    
    final Collection<Service> services = getServices(mock, cenlProvider);
    when(mock.createStore(Matchers.<Store.Configuration>anyObject())).thenReturn(mock(Store.class));
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder()
        .build(), services);
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
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider mock = mock(Store.Provider.class);
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(any(Store.class))).thenReturn(cenlServiceMock);
    
    final Collection<Service> services = getServices(mock, cenlProvider);
    when(mock.createStore(Matchers.<Store.Configuration>anyObject())).thenReturn(mock(Store.class));
    final String cacheAlias = "bar";
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache(cacheAlias,
        cacheConfiguration)
        .build(), services);
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
    final Set<Cache<?,?>> caches = new HashSet<Cache<?, ?>>();
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider storeProvider = mock(Store.Provider.class);
    final Collection<Service> services = getServices(storeProvider, null);
    final RuntimeException thrown = new RuntimeException();
    when(storeProvider.createStore(Matchers.<Store.Configuration>anyObject())).thenReturn(mock(Store.class));
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder()
        .addCache("foo", cacheConfiguration)
        .addCache("bar", cacheConfiguration)
        .addCache("foobar", cacheConfiguration)
        .build(), services) {

      @Override
      <K, V> Ehcache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                            final Class<K> keyType, final Class<V> valueType) {
        final Ehcache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        caches.add(ehcache);
        if(caches.size() == 1) {
          when(storeProvider.createStore(Matchers.<Store.Configuration<K,V>>anyObject(),
              Matchers.<ServiceConfiguration<?>>anyVararg()))
              .thenThrow(thrown);
        }
        return ehcache;
      }

      @Override
      void closeEhcache(final String alias, final Ehcache<?, ?> ehcache) {
        super.closeEhcache(alias, ehcache);
        caches.remove(ehcache);
      }
    };

    try {
      cacheManager.init();
      fail();
    } catch (StateTransitionException e) {
      assertThat(cacheManager.getStatus(), is(Status.UNINITIALIZED));
      assertThat(e.getCause().getMessage(), CoreMatchers.startsWith("Cache '"));
      assertThat(e.getCause().getMessage(), CoreMatchers.endsWith("' creation in EhcacheManager failed."));
      
    }
    assertThat(caches.isEmpty(), is(true));
  }

  @Test
  public void testClosesAllCachesDownWhenCloseThrows() {
    final Set<String> caches = new HashSet<String>();
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider storeProvider = mock(Store.Provider.class);
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = mock(CacheEventNotificationListenerServiceProvider.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(any(Store.class))).thenReturn(cenlServiceMock);
    
    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    final RuntimeException thrown = new RuntimeException();
    when(storeProvider.createStore(Matchers.<Store.Configuration>anyObject())).thenReturn(mock(Store.class));
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder()
        .addCache("foo", cacheConfiguration)
        .addCache("bar", cacheConfiguration)
        .addCache("foobar", cacheConfiguration)
        .build(), services) {

      @Override
      <K, V> Ehcache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config,
                                            final Class<K> keyType, final Class<V> valueType) {
        final Ehcache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        caches.add(alias);
        return ehcache;
      }

      @Override
      void closeEhcache(final String alias, final Ehcache<?, ?> ehcache) {
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
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder()
        .build(), getServices(null, null));
    final CacheManagerListener listener = mock(CacheManagerListener.class);
    cacheManager.registerListener(listener);
    cacheManager.init();
    verify(listener).stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    cacheManager.close();
    verify(listener).stateTransition(Status.AVAILABLE, Status.UNINITIALIZED);
  }
  
  @Test
  public void testCloseNoLoaderWriterAndCacheEventListener() throws Exception {
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    final Store.Provider storeProvider = spy(new Store.Provider() {
      @Override
      public void stop() {
      }

      @Override
      public void start(ServiceProvider serviceProvider) {
      }

      @Override
      public void releaseStore(Store<?, ?> resource) {

      }

      @Override
      public void initStore(Store<?, ?> resource) {

      }

      @Override
      public <K, V> Store<K, V> createStore(org.ehcache.spi.cache.Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
        return null;
      }
    });
    
    final CacheEventNotificationListenerServiceProvider cenlProvider = spy(new CacheEventNotificationListenerServiceProvider() {
      @Override
      public void start(ServiceProvider serviceProvider) {
      }

      @Override
      public void stop() {
      }

      @Override
      public <K, V> CacheEventNotificationService<K, V> createCacheEventNotificationService(Store<K, V> store, ServiceConfiguration<?>... serviceConfigs) {
        return null;
      }

      @Override
      public <K, V> void releaseCacheEventNotificationService(CacheEventNotificationService<K, V> cenlService) {
        cenlService.releaseAllListeners();
      }
    });
    Store mockStore = mock(Store.class);
    final CacheEventNotificationService<Object, Object> cenlServiceMock = mock(CacheEventNotificationServiceImpl.class);
    when(cenlProvider.createCacheEventNotificationService(mockStore)).thenReturn(cenlServiceMock);
    final Collection<Service> services = getServices(storeProvider, cenlProvider);
    List<CacheConfigurationChangeListener> configurationChangeListenerList = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.add(mock(CacheConfigurationChangeListener.class));
    when(mockStore.getConfigurationChangeListeners()).thenReturn(configurationChangeListenerList);
    when(storeProvider.createStore(Matchers.<Store.Configuration>anyObject())).thenReturn(mockStore);
    
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("foo", cacheConfiguration).build(), services) {
      @Override
      <K, V> Ehcache<K, V> createNewEhcache(final String alias, final CacheConfiguration<K, V> config, final Class<K> keyType, final Class<V> valueType) {
        final Ehcache<K, V> ehcache = super.createNewEhcache(alias, config, keyType, valueType);
        return spy(ehcache);
      }
    };
    cacheManager.init();
    Ehcache<Object, Object> testCache = (Ehcache<Object, Object>) cacheManager.getCache("foo", Object.class, Object.class);
    cacheManager.close();
    verify(testCache).close();
    verify(cenlServiceMock, times(1)).releaseAllListeners();
  }

  private Collection<Service> getServices(Store.Provider storeProvider, CacheEventNotificationListenerServiceProvider cenlProvider) {
    return getServices(mock(CacheLoaderWriterProvider.class), mock(WriteBehindDecoratorLoaderWriterProvider.class),
        storeProvider != null ? storeProvider : mock(Store.Provider.class),
        cenlProvider != null ? cenlProvider : mock(CacheEventNotificationListenerServiceProvider.class));
  }

  private Collection<Service> getServices(CacheLoaderWriterProvider cacheLoaderWriterProvider,
                                     WriteBehindDecoratorLoaderWriterProvider decoratorLoaderWriterProvider,
                                     Store.Provider storeProvider,
                                     CacheEventNotificationListenerServiceProvider cenlProvider) {
    return new ArrayList<Service>(Arrays.asList(cacheLoaderWriterProvider, storeProvider, decoratorLoaderWriterProvider, cenlProvider, mock(CacheEventListenerProvider.class)));
  }

  static class NoSuchService implements Service {

    @Override
    public void start(final ServiceProvider serviceProvider) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

}
