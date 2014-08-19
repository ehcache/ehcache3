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

package org.ehcache.internal;

import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Alex Snaps
 */
public class HeapResourceFactory implements ServiceFactory<HeapResource> {

  @Override
  public HeapResource create(final ServiceConfiguration<HeapResource> serviceConfiguration, final ServiceLocator serviceLocator) {
    return new HeapResource(serviceLocator);
  }

  @Override
  public Class<HeapResource> getServiceType() {
    return HeapResource.class;
  }
}
