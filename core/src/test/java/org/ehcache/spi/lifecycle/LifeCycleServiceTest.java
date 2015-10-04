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

package org.ehcache.spi.lifecycle;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.EhcacheManager;
import org.ehcache.Maintainable;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.events.CacheEventNotificationListenerServiceProvider;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.StateChangeListener;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindDecoratorLoaderWriterProvider;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.util.Deferred;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.calls;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Mathieu Carbou
 */
public class LifeCycleServiceTest {

  @Test
  public void testServicesCanListenOnLifeCycles() {

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    MyService myService = new MyService();

    Store.Provider storeProvider = mock(Store.Provider.class);
    Store store = mock(Store.class);
    CacheEventNotificationListenerServiceProvider cacheEventNotificationListenerServiceProvider = mock(CacheEventNotificationListenerServiceProvider.class);

    when(storeProvider.createStore(any(Store.Configuration.class), Matchers.<ServiceConfiguration>anyVararg())).thenReturn(store);
    when(store.getConfigurationChangeListeners()).thenReturn(new ArrayList<CacheConfigurationChangeListener>());
    when(cacheEventNotificationListenerServiceProvider.createCacheEventNotificationService(store)).thenReturn(mock(CacheEventNotificationService.class));

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(myService)
        .using(storeProvider)
        .using(mock(CacheLoaderWriterProvider.class))
        .using(mock(WriteBehindDecoratorLoaderWriterProvider.class))
        .using(cacheEventNotificationListenerServiceProvider)
        .using(mock(CacheEventListenerProvider.class))
        .using(mock(LocalPersistenceService.class))
        .build(true);

    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init")));

    cacheManager.close();
    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init", "cache-closed", "stop", "cache-manager-closed")));

    cacheManager.init();
    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init", "cache-closed", "stop", "cache-manager-closed", "start", "cache-init", "cache-manager-init")));

    cacheManager.close();
    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init", "cache-closed", "stop", "cache-manager-closed", "start", "cache-init", "cache-manager-init", "cache-closed", "stop", "cache-manager-closed")));

    myService.calls.clear();

    Maintainable maintainable = ((PersistentCacheManager) cacheManager).toMaintenance();
    assertThat(myService.calls, is(empty()));

    // maintenance mode does not trigger cache / cache manager lifecycle events 
    maintainable.close();
    assertThat(myService.calls, is(empty()));

    cacheManager.init();
    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init")));

    cacheManager.close();
    assertThat(myService.calls, equalTo(asList("start", "cache-init", "cache-manager-init", "cache-closed", "stop", "cache-manager-closed")));
  }

  @Test
  public void testCanUseAdapter() {
    final List<String> calls = new ArrayList<String>();
    LifeCycleListenerAdapter<String> adapter1 = new LifeCycleListenerAdapter<String>() {
    };
    LifeCycleListenerAdapter<String> adapter2 = new LifeCycleListenerAdapter<String>() {
      @Override
      public void afterInitialization(String instance) {
        calls.add("adapter2-init");
        assertThat(instance, equalTo("data"));
      }
    };
    LifeCycleListenerAdapter<String> adapter3 = new LifeCycleListenerAdapter<String>() {
      @Override
      public void afterClosing(String instance) {
        calls.add("adapter3-close");
        assertThat(instance, equalTo("data"));
      }
    };
    LifeCycleListenerAdapter<String> adapter4 = new LifeCycleListenerAdapter<String>() {
      @Override
      public void afterInitialization(String instance) {
        calls.add("adapter4-init");
        assertThat(instance, equalTo("data"));
      }

      @Override
      public void afterClosing(String instance) {
        calls.add("adapter4-close");
        assertThat(instance, equalTo("data"));
      }
    };
    DefaultLifeCycleManager lifeCycleManager = new DefaultLifeCycleManager();
    lifeCycleManager.register(String.class, adapter1);
    lifeCycleManager.register(String.class, adapter2);
    lifeCycleManager.register(String.class, adapter3);
    lifeCycleManager.register(String.class, adapter4);

    StateChangeListener changeListener = lifeCycleManager.createStateChangeListener("data");

    lifeCycleManager.start(null);
    changeListener.stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    assertThat(calls, equalTo(asList("adapter2-init", "adapter4-init")));

    lifeCycleManager.stop();
    changeListener.stateTransition(Status.AVAILABLE, Status.UNINITIALIZED);
    assertThat(calls, equalTo(asList("adapter2-init", "adapter4-init", "adapter4-close", "adapter3-close")));

    lifeCycleManager.start(null);
    changeListener.stateTransition(Status.UNINITIALIZED, Status.MAINTENANCE);
    assertThat(calls, equalTo(asList("adapter2-init", "adapter4-init", "adapter4-close", "adapter3-close")));

    lifeCycleManager.stop();
    changeListener.stateTransition(Status.MAINTENANCE, Status.UNINITIALIZED);
    assertThat(calls, equalTo(asList("adapter2-init", "adapter4-init", "adapter4-close", "adapter3-close")));
  }

  @Test
  public void testCanUseDeferred() {
    DefaultLifeCycleManager lifeCycleManager = new DefaultLifeCycleManager();
    
    DeferredLifeCycleListener<String> deferred = new DeferredLifeCycleListener<String>();

    Deferred.Consumer<String> consumer = mock(Deferred.Consumer.class);
    
    deferred.afterInitialization().done(consumer);
    deferred.afterClosing().done(consumer);

    lifeCycleManager.register(String.class, deferred);
    
    StateChangeListener changeListener = lifeCycleManager.createStateChangeListener("data");
    lifeCycleManager.start(null);
    verifyZeroInteractions(consumer);
    
    changeListener.stateTransition(Status.UNINITIALIZED, Status.AVAILABLE);
    verify(consumer, times(1)).consume("data");
    
    changeListener.stateTransition(Status.AVAILABLE, Status.UNINITIALIZED);
    verify(consumer, times(2)).consume("data");
  }

  @ServiceDependencies(LifeCycleService.class)
  static class MyService implements Service {

    List<String> calls = new ArrayList<String>();

    @Override
    public void start(ServiceProvider serviceProvider) {
      calls.add("start");

      final LifeCycleService lifeCycleService = serviceProvider.getService(LifeCycleService.class);

      // could listen to Cache, Ehcache, etc.
      final LifeCycleListener<Cache> cacheListener = new LifeCycleListener<Cache>() {
        @Override
        public void afterInitialization(Cache instance) {
          calls.add("cache-init");
        }

        @Override
        public void afterClosing(Cache instance) {
          calls.add("cache-closed");
        }
      };

      lifeCycleService.register(Cache.class, cacheListener);

      // could listen to CacheManager, EhCacheManager, etc
      lifeCycleService.register(EhcacheManager.class, new LifeCycleListener<EhcacheManager>() {
        @Override
        public void afterInitialization(EhcacheManager instance) {
          calls.add("cache-manager-init");
        }

        @Override
        public void afterClosing(EhcacheManager instance) {
          calls.add("cache-manager-closed");

          //lifeCycleService.unregister(this);
          //lifeCycleService.unregister(cacheListener);
        }
      });
    }

    @Override
    public void stop() {
      calls.add("stop");
    }
  }

}
