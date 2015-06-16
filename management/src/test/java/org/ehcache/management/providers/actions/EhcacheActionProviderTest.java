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
package org.ehcache.management.providers.actions;

import org.ehcache.Ehcache;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.CallDescriptor;
import org.terracotta.management.capabilities.descriptors.Descriptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class EhcacheActionProviderTest {

  @Test
  public void testDescriptions() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider();

    ehcacheActionProvider.register(mock(Ehcache.class));

    Set<Descriptor> descriptions = ehcacheActionProvider.descriptions();
    assertThat(descriptions.size(), is(4));
    assertThat(descriptions, (Matcher) containsInAnyOrder(
        new CallDescriptor("remove", "void", Collections.singletonList(new CallDescriptor.Parameter("key", "java.lang.Object"))),
        new CallDescriptor("get", "java.lang.Object", Collections.singletonList(new CallDescriptor.Parameter("key", "java.lang.Object"))),
        new CallDescriptor("put", "void", Arrays.asList(new CallDescriptor.Parameter("key", "java.lang.Object"), new CallDescriptor.Parameter("value", "java.lang.Object"))),
        new CallDescriptor("clear", "void", Collections.<CallDescriptor.Parameter>emptyList())
    ));
  }

  @Test
  public void testCapabilityContext() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider();

    ehcacheActionProvider.register(mock(Ehcache.class));

    CapabilityContext capabilityContext = ehcacheActionProvider.capabilityContext();

    assertThat(capabilityContext.getAttributes().size(), is(2));

    Iterator<CapabilityContext.Attribute> iterator = capabilityContext.getAttributes().iterator();
    CapabilityContext.Attribute next = iterator.next();
    assertThat(next.getName(), equalTo("cacheManagerName"));
    assertThat(next.isRequired(), is(true));
    next = iterator.next();
    assertThat(next.getName(), equalTo("cacheName"));
    assertThat(next.isRequired(), is(true));
  }

  @Test
  public void testCollectStatistics() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider();

    try {
      ehcacheActionProvider.collectStatistics(null, null);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }
  }

  @Test
  public void testCallAction_happyPathNoParam() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(ehcache);


    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-0");
    context.put("cacheName", "cache-0");

    ehcacheActionProvider.callAction(context, "clear", new String[0], new Object[0]);

    verify(ehcache, times(1)).clear();
  }

  @Test
  public void testCallAction_happyPathWithParams() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(cacheRuntimeConfiguration.getKeyType()).thenReturn(Long.class);
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(ehcache);


    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-0");
    context.put("cacheName", "cache-0");

    ehcacheActionProvider.callAction(context, "get", new String[]{"java.lang.Object"}, new Object[]{"1"});

    verify(ehcache, times(1)).get(eq(1L));
  }

  @Test
  public void testCallAction_noSuchCache() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    ehcacheActionProvider.register(ehcache);

    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-0");
    context.put("cacheName", "cache-1");

    try {
      ehcacheActionProvider.callAction(context, "clear", new String[0], new Object[0]);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).clear();
  }

  @Test
  public void testCallAction_noSuchCacheManager() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    ehcacheActionProvider.register(ehcache);

    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-1");
    context.put("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "clear", new String[0], new Object[0]);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).clear();
  }

  @Test
  public void testCallAction_noSuchMethodName() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(ehcache);

    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-0");
    context.put("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "clearer", new String[0], new Object[0]);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testCallAction_noSuchMethod() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider() {
      @Override
      String findCacheName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-0";
      }

      @Override
      String findCacheManagerName(Map.Entry<Ehcache, EhcacheActionWrapper> entry) {
        return "cache-manager-0";
      }
    };

    Ehcache ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(ehcache);

    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "cache-manager-0");
    context.put("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "get", new String[]{"java.lang.Long"}, new Object[]{0L});
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).get(null);
  }

}
