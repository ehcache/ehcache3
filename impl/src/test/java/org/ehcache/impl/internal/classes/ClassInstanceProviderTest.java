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
package org.ehcache.impl.internal.classes;

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Ludovic Orban
 */
public class ClassInstanceProviderTest {

  @SuppressWarnings("unchecked")
  private Class<ClassInstanceConfiguration<TestService>> configClass = (Class)ClassInstanceConfiguration.class;

  @Test
  public void testNewInstanceUsingAliasAndNoArgs() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    classInstanceProvider.preconfigured.put("test stuff", new ClassInstanceConfiguration<TestService>(TestService.class));
    TestService obj = classInstanceProvider.newInstance("test stuff", (ServiceConfiguration) null);

    assertThat(obj.theString, is(nullValue()));
  }

  @Test
  public void testNewInstanceUsingAliasAndArg() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    classInstanceProvider.preconfigured.put("test stuff", new ClassInstanceConfiguration<>(TestService.class, "test string"));
    TestService obj = classInstanceProvider.newInstance("test stuff", (ServiceConfiguration<?>) null);

    assertThat(obj.theString, equalTo("test string"));
  }

  @Test
  public void testNewInstanceUsingServiceConfig() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    TestServiceConfiguration config = new TestServiceConfiguration();
    TestService obj = classInstanceProvider.newInstance("test stuff", config);

    assertThat(obj.theString, is(nullValue()));
  }

  @Test
  public void testNewInstanceUsingServiceConfigFactory() throws Exception {
    TestServiceProviderConfiguration factoryConfig = new TestServiceProviderConfiguration();
    factoryConfig.getDefaults().put("test stuff", new ClassInstanceConfiguration<TestService>(TestService.class));

    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(factoryConfig, configClass);
    classInstanceProvider.start(null);

    TestService obj = classInstanceProvider.newInstance("test stuff", (ServiceConfiguration) null);
    assertThat(obj.theString, is(nullValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReleaseInstanceByAnotherProvider() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<String>, String> classInstanceProvider = new ClassInstanceProvider<>(null, null);

    classInstanceProvider.releaseInstance("foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReleaseSameInstanceMultipleTimesThrows() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<String>, String> classInstanceProvider = new ClassInstanceProvider<>(null, null);
    classInstanceProvider.providedVsCount.put("foo", new AtomicInteger(1));

    classInstanceProvider.releaseInstance("foo");
    classInstanceProvider.releaseInstance("foo");
  }

  @Test
  public void testReleaseCloseableInstance() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<Closeable>, Closeable> classInstanceProvider = new ClassInstanceProvider<>(null, null);
    Closeable closeable = mock(Closeable.class);
    classInstanceProvider.providedVsCount.put(closeable, new AtomicInteger(1));
    classInstanceProvider.instantiated.add(closeable);

    classInstanceProvider.releaseInstance(closeable);
    verify(closeable).close();
  }

  @Test(expected = IOException.class)
  public void testReleaseCloseableInstanceThrows() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<Closeable>, Closeable> classInstanceProvider = new ClassInstanceProvider<>(null, null);
    Closeable closeable = mock(Closeable.class);
    doThrow(IOException.class).when(closeable).close();
    classInstanceProvider.providedVsCount.put(closeable, new AtomicInteger(1));
    classInstanceProvider.instantiated.add(closeable);

    classInstanceProvider.releaseInstance(closeable);
  }

  @Test
  public void testNewInstanceWithActualInstanceInServiceConfig() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    TestService service = new TestService();
    TestServiceConfiguration config = new TestServiceConfiguration(service);

    TestService newService = classInstanceProvider.newInstance("test stuff", config);

    assertThat(newService, sameInstance(service));
  }

  @Test
  public void testSameInstanceRetrievedMultipleTimesUpdatesTheProvidedCount() throws Exception {
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestService>, TestService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    TestService service = new TestService();
    TestServiceConfiguration config = new TestServiceConfiguration(service);

    TestService newService = classInstanceProvider.newInstance("test stuff", config);
    assertThat(newService, sameInstance(service));
    assertThat(classInstanceProvider.providedVsCount.get(service).get(), is(1));
    newService = classInstanceProvider.newInstance("test stuff", config);
    assertThat(newService, sameInstance(service));
    assertThat(classInstanceProvider.providedVsCount.get(service).get(), is(2));
  }

  @Test
  public void testInstancesNotCreatedByProviderDoesNotClose() throws IOException {
    @SuppressWarnings("unchecked")
    Class<ClassInstanceConfiguration<TestCloseableService>> configClass = (Class) ClassInstanceConfiguration.class;
    ClassInstanceProvider<String, ClassInstanceConfiguration<TestCloseableService>, TestCloseableService> classInstanceProvider = new ClassInstanceProvider<>(null, configClass);

    TestCloseableService service = mock(TestCloseableService.class);
    TestCloaseableServiceConfig config = new TestCloaseableServiceConfig(service);

    TestCloseableService newService = classInstanceProvider.newInstance("testClose", config);
    assertThat(newService, sameInstance(service));
    classInstanceProvider.releaseInstance(newService);
    verify(service, times(0)).close();

  }


  public static abstract class TestCloseableService implements Service, Closeable {

  }

  public static class TestCloaseableServiceConfig extends ClassInstanceConfiguration<TestCloseableService> implements ServiceConfiguration<TestCloseableService> {

    public TestCloaseableServiceConfig() {
      super(TestCloseableService.class);
    }

    public TestCloaseableServiceConfig(TestCloseableService testCloseableService) {
      super(testCloseableService);
    }

    @Override
    public Class<TestCloseableService> getServiceType() {
      return TestCloseableService.class;
    }
  }

  public static class TestService implements Service {
    public final String theString;

    public TestService() {
      this(null);
    }

    public TestService(String theString) {
      this.theString = theString;
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
    }

    @Override
    public void stop() {
    }
  }

  public static class TestServiceConfiguration extends ClassInstanceConfiguration<TestService> implements ServiceConfiguration<TestService> {
    public TestServiceConfiguration() {
      super(TestService.class);
    }

    public TestServiceConfiguration(TestService service) {
      super(service);
    }

    @Override
    public Class<TestService> getServiceType() {
      return TestService.class;
    }
  }

  public static class TestServiceProviderConfiguration extends ClassInstanceProviderConfiguration<String, ClassInstanceConfiguration<TestService>> implements ServiceConfiguration<TestService> {
    @Override
    public Class<TestService> getServiceType() {
      return TestService.class;
    }
  }

}
