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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.internal.service.ClusteringServiceFactory;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.core.internal.util.ClassLoading;
import org.junit.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class ClusteringServiceFactoryTest {

  @Test
  public void testServiceLocator() throws Exception {
    final String expectedFactory = ClusteringServiceFactory.class.getName();
    final ServiceLoader<ServiceFactory> factories = ClassLoading.libraryServiceLoaderFor(ServiceFactory.class);
    foundParser: {
      for (final ServiceFactory factory : factories) {
        if (factory.getClass().getName().equals(expectedFactory)) {
          break foundParser;
        }
      }
      fail("Expected factory not found");
    }
  }

}