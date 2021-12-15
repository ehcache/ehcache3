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
package org.ehcache.core.spi.services;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceProvider;

public class TestMandatoryServiceFactory implements ServiceFactory<TestMandatoryServiceFactory.TestMandatoryService> {

  @Override
  public boolean isMandatory() {
    return true;
  }

  @Override
  public TestMandatoryService create(ServiceCreationConfiguration<TestMandatoryService, ?> configuration) {
    if (configuration == null) {
      return new TestMandatoryService(null);
    } else {
      return new TestMandatoryService(((TestMandatoryServiceConfiguration) configuration).getConfig());
    }
  }

  @Override
  public Class<? extends TestMandatoryService> getServiceType() {
    return TestMandatoryService.class;
  }

  public static class TestMandatoryServiceConfiguration implements ServiceCreationConfiguration<TestMandatoryService, Void> {

    private final String config;

    public TestMandatoryServiceConfiguration(String config) {
      this.config = config;
    }

    @Override
    public Class<TestMandatoryService> getServiceType() {
      return TestMandatoryService.class;
    }

    public String getConfig() {
      return config;
    }
  }

  public static class TestMandatoryService implements Service {

    private final String config;

    public TestMandatoryService(String config) {
      this.config = config;
    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {

    }

    @Override
    public void stop() {

    }

    public String getConfig() {
      return config;
    }
  }
}
