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

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;

/**
 * DefaultTestService
 */
public class DefaultTestProvidedService implements TestProvidedService {
  int starts;
  int stops;
  static int ctors;


  public DefaultTestProvidedService() {
    ctors++;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    starts++;
  }

  @Override
  public void stop() {
    stops++;
  }

  @Override
  public int starts() {
    return starts;
  }

  @Override
  public int stops() {
    return stops;
  }

  @Override
  public int ctors() {
    return ctors;
  }
}
