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
import org.ehcache.config.Configuration;
import org.ehcache.config.ConfigurationBuilder;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.ConfigurationBuilder.newConfigurationBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EhcacheManagerTest {

  @Test
  public void testConstructorThrowsWhenNotBeingToResolveService() {
    final Configuration config = newConfigurationBuilder().addService(new ServiceConfiguration<NoSuchService>() {
      @Override
      public Class<NoSuchService> getServiceType() {
        return NoSuchService.class;
      }
    }).build();
    try {
      new EhcacheManager(config);
      fail("Should have thrown...");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(NoSuchService.class.getName()));
    }
  }
  
  @Test
  public void testNoClassLoaderSpecified() {
    ConfigurationBuilder builder = newConfigurationBuilder();
    builder.addCache("foo", newCacheConfigurationBuilder().buildCacheConfig(Object.class, Object.class));
    EhcacheManager cacheManager = new EhcacheManager(builder.build());
    assertSame(ClassLoading.getDefaultClassLoader(), cacheManager.getClassLoader());
    assertSame(cacheManager.getClassLoader(), cacheManager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
    
    // explicit null
    builder = newConfigurationBuilder();
    builder.withClassLoader(null);
    builder.addCache("foo", newCacheConfigurationBuilder().buildCacheConfig(Object.class, Object.class));
    cacheManager = new EhcacheManager(builder.build());
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
    
    ConfigurationBuilder builder = newConfigurationBuilder().withClassLoader(cl1);
    
    // these caches should inherit the cache manager classloader
    builder.addCache("foo1", newCacheConfigurationBuilder().buildConfig(Object.class, Object.class));
    builder.addCache("foo2", newCacheConfigurationBuilder().withClassLoader(null).buildConfig(Object.class, Object.class));
    
    // this cache specifies its own unique classloader
    builder.addCache("foo3", newCacheConfigurationBuilder().withClassLoader(cl2).buildConfig(Object.class, Object.class));
    
    EhcacheManager cacheManager = new EhcacheManager(builder.build());
    assertSame(cl1, cacheManager.getClassLoader());
    assertSame(cl1, cacheManager.getCache("foo1", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
    assertSame(cl1, cacheManager.getCache("foo2", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
    assertSame(cl2, cacheManager.getCache("foo3", Object.class, Object.class).getRuntimeConfiguration().getClassLoader());
  }
  

  @Test
  public void testReturnsNullForNonExistCache() {
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().build());
    assertThat(cacheManager.getCache("foo", Object.class, Object.class), nullValue());
  }

  @Test
  public void testThrowsWhenAddingExistingCache() {
    final CacheConfiguration<Object, Object> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Object.class, Object.class);
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("bar",
        cacheConfiguration)
        .build());
    final Cache<Object, Object> cache = cacheManager.getCache("bar", Object.class, Object.class);
    try {
      cacheManager.createCache("bar", cacheConfiguration);
      fail("Should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("bar"));
    }
  }

  @Test
  public void testThrowsWhenRetrievingCacheWithWrongTypes() {
    final CacheConfiguration<Integer, String> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Integer.class, String.class);
    EhcacheManager cacheManager = new EhcacheManager(newConfigurationBuilder().addCache("bar",
        cacheConfiguration)
        .build());
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

    final CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);

    final CacheConfiguration<Long, Long> barConfig = mock(CacheConfiguration.class);
    final CacheConfiguration<Integer, CharSequence> fooConfig = mock(CacheConfiguration.class);

    CacheLoader fooLoader = mock(CacheLoader.class);

    when(cacheLoaderFactory.createCacheLoader("foo", fooConfig)).thenReturn(fooLoader);

    final Configuration cfg = new Configuration(
        new HashMap<String, CacheConfiguration<?, ?>>() {{
          put("bar", barConfig);
          put("foo", fooConfig);
        }},
        getClass().getClassLoader()
    );

    final EhcacheManager manager = new EhcacheManager(cfg, new ServiceLocator(cacheLoaderFactory, new OnHeapStore.Provider()));

    verify(cacheLoaderFactory).createCacheLoader("bar", barConfig);
    verify(cacheLoaderFactory).createCacheLoader("foo", fooConfig);

    manager.removeCache("bar");
    verify(cacheLoaderFactory, never()).releaseCacheLoader((CacheLoader<?, ?>)Mockito.anyObject());
    manager.removeCache("foo");
    verify(cacheLoaderFactory).releaseCacheLoader(fooLoader);
  }

  static class NoSuchService implements Service {

    @Override
    public void stop() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

}