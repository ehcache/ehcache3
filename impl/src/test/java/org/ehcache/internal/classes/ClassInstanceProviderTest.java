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
package org.ehcache.internal.classes;

import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 * @author Ludovic Orban
 */
public class ClassInstanceProviderTest {

  @Test
  public void testNewInstanceUsingAliasAndNoArgs() throws Exception {
    ClassInstanceProvider<TestService> classInstanceProvider = new ClassInstanceProvider<TestService>((Class)ClassInstanceProviderFactoryConfiguration.class, (Class)ClassInstanceProviderConfiguration.class);

    classInstanceProvider.preconfiguredLoaders.put("test stuff", TestService.class);
    TestService obj = classInstanceProvider.newInstance("test stuff", (ServiceConfiguration) null);

    assertThat(obj.theString, is(nullValue()));
  }

  @Test
  public void testNewInstanceUsingAliasAndArg() throws Exception {
    ClassInstanceProvider<TestService> classInstanceProvider = new ClassInstanceProvider<TestService>((Class)ClassInstanceProviderFactoryConfiguration.class, (Class)ClassInstanceProviderConfiguration.class);

    classInstanceProvider.preconfiguredLoaders.put("test stuff", TestService.class);
    TestService obj = classInstanceProvider.newInstance("test stuff", null, new ClassInstanceProvider.ConstructorArgument<String>(String.class, "test string"));

    assertThat(obj.theString, equalTo("test string"));
  }

  @Test
  public void testNewInstanceUsingServiceConfig() throws Exception {
    ClassInstanceProvider<TestService> classInstanceProvider = new ClassInstanceProvider<TestService>((Class)ClassInstanceProviderFactoryConfiguration.class, (Class)ClassInstanceProviderConfiguration.class);

    TestServiceProviderConfiguration config = new TestServiceProviderConfiguration();
    TestService obj = classInstanceProvider.newInstance("test stuff", config);

    assertThat(obj.theString, is(nullValue()));
  }

  @Test
  public void testNewInstanceUsingServiceConfigFactory() throws Exception {
    ClassInstanceProvider<TestService> classInstanceProvider = new ClassInstanceProvider<TestService>((Class)ClassInstanceProviderFactoryConfiguration.class, (Class)ClassInstanceProviderConfiguration.class);

    TestServiceProviderFactoryConfiguration factoryConfig = new TestServiceProviderFactoryConfiguration();
    factoryConfig.getDefaults().put("test stuff", TestService.class);

    classInstanceProvider.start(factoryConfig, null);

    TestService obj = classInstanceProvider.newInstance("test stuff", (ServiceConfiguration) null);
    assertThat(obj.theString, is(nullValue()));
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
    public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
    }

    @Override
    public void stop() {
    }
  }

  public static class TestServiceProviderConfiguration extends ClassInstanceProviderConfiguration<TestService> implements ServiceConfiguration<TestService> {
    public TestServiceProviderConfiguration() {
      super(TestService.class);
    }

    @Override
    public Class<TestService> getServiceType() {
      return TestService.class;
    }
  }

  public static class TestServiceProviderFactoryConfiguration extends ClassInstanceProviderFactoryConfiguration<TestService> implements ServiceConfiguration<TestService> {
    @Override
    public Class<TestService> getServiceType() {
      return TestService.class;
    }
  }

}
