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

import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.core.Ehcache;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.Test;
import org.mockito.Mockito;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.capabilities.context.CapabilityContext;
import org.terracotta.management.model.capabilities.descriptors.CallDescriptor;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.context.Context;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EhcacheActionProviderTest {

  Context cmContext = Context.create("cacheManagerName", "myCacheManagerName");
  Context cmContext_0 = Context.create("cacheManagerName", "cache-manager-0");
  ManagementRegistryServiceConfiguration cmConfig = new DefaultManagementRegistryConfiguration().setContext(cmContext);
  ManagementRegistryServiceConfiguration cmConfig_0 = new DefaultManagementRegistryConfiguration().setContext(cmContext_0);

  @Test
  @SuppressWarnings("unchecked")
  public void testDescriptions() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig);

    ehcacheActionProvider.register(new CacheBinding("myCacheName1", mock(Ehcache.class)));
    ehcacheActionProvider.register(new CacheBinding("myCacheName2", mock(Ehcache.class)));

    Collection<? extends Descriptor> descriptions = ehcacheActionProvider.getDescriptors();
    assertThat(descriptions.size(), is(4));
    assertThat(descriptions, containsInAnyOrder(
        new CallDescriptor("remove", "void", Collections.singletonList(new CallDescriptor.Parameter("key", "java.lang.Object"))),
        new CallDescriptor("get", "java.lang.Object", Collections.singletonList(new CallDescriptor.Parameter("key", "java.lang.Object"))),
        new CallDescriptor("put", "void", Arrays.asList(new CallDescriptor.Parameter("key", "java.lang.Object"), new CallDescriptor.Parameter("value", "java.lang.Object"))),
        new CallDescriptor("clear", "void", Collections.emptyList())
    ));
  }

  @Test
  public void testCapabilityContext() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig);

    ehcacheActionProvider.register(new CacheBinding("myCacheName1", mock(Ehcache.class)));
    ehcacheActionProvider.register(new CacheBinding("myCacheName2", mock(Ehcache.class)));

    CapabilityContext capabilityContext = ehcacheActionProvider.getCapabilityContext();

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
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig);

    try {
      ehcacheActionProvider.collectStatistics(null, null, 0);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }
  }

  @Test
  public void testCallAction_happyPathNoParam() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    Ehcache<Object, Object> ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration<Object, Object> cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));

    Context context = cmContext_0.with("cacheName", "cache-0");

    ehcacheActionProvider.callAction(context, "clear", Void.class);

    verify(ehcache, times(1)).clear();
  }

  @Test
  public void testCallAction_happyPathWithParams() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    @SuppressWarnings("unchecked")
    Ehcache<Long, String> ehcache = mock(Ehcache.class);
    @SuppressWarnings("unchecked")
    CacheRuntimeConfiguration<Long, String> cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(cacheRuntimeConfiguration.getKeyType()).thenReturn(Long.class);
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));


    Context context = cmContext_0.with("cacheName", "cache-0");

    ehcacheActionProvider.callAction(context, "get", Object.class, new Parameter("1", Object.class.getName()));

    verify(ehcache, times(1)).get(eq(1L));
  }

  @Test
  public void testCallAction_noSuchCache() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    Ehcache<?, ?> ehcache = mock(Ehcache.class);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));

    Context context = cmContext_0.with("cacheName", "cache-1");

    try {
      ehcacheActionProvider.callAction(context, "clear", Void.class);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).clear();
  }

  @Test
  public void testCallAction_noSuchCacheManager() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    Ehcache<?, ?> ehcache = mock(Ehcache.class);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));

    Context context = Context.empty()
        .with("cacheManagerName", "cache-manager-1")
        .with("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "clear", Void.class);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).clear();
  }

  @Test
  public void testCallAction_noSuchMethodName() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    Ehcache<Object, Object> ehcache = mock(Ehcache.class);
    CacheRuntimeConfiguration<Object, Object> cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));

    Context context = cmContext_0.with("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "clearer", Void.class);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testCallAction_noSuchMethod() throws Exception {
    EhcacheActionProvider ehcacheActionProvider = new EhcacheActionProvider(cmConfig_0);

    @SuppressWarnings("unchecked")
    Ehcache<Long, String> ehcache = mock(Ehcache.class);
    @SuppressWarnings("unchecked")
    CacheRuntimeConfiguration<Long, String> cacheRuntimeConfiguration = mock(CacheRuntimeConfiguration.class);
    when(cacheRuntimeConfiguration.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
    when(ehcache.getRuntimeConfiguration()).thenReturn(cacheRuntimeConfiguration);
    ehcacheActionProvider.register(new CacheBinding("cache-0", ehcache));

    Context context = Context.empty()
        .with("cacheManagerName", "cache-manager-1")
        .with("cacheName", "cache-0");

    try {
      ehcacheActionProvider.callAction(context, "get", Object.class, new Parameter(0L, Long.class.getName()));
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    verify(ehcache, times(0)).get(null);
  }

  @SuppressWarnings("unchecked")
  private static <T> T mock(Class<?> clazz) {
    return Mockito.mock((Class<T>) clazz);
  }
}
