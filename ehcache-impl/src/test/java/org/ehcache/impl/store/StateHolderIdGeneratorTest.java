/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.store;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.impl.config.SizedResourcePoolImpl;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.internal.store.shared.StateHolderIdGenerator;
import org.ehcache.impl.persistence.DefaultDiskResourceService;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class StateHolderIdGeneratorTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private ServiceLocator serviceLocator;
  private StateHolderIdGenerator<String> sharedPersistence;
  private PersistableResourceService.PersistenceSpaceIdentifier<?> spaceIdentifier;
  private DefaultDiskResourceService diskResourceService;
  private ResourcePool diskResource;

  @Before
  public void before() throws Throwable {
    File folder = new File(temporaryFolder.newFolder().getAbsolutePath());
    diskResourceService = new DefaultDiskResourceService();
    ServiceLocator.DependencySet dependencySet = dependencySet();
    dependencySet.with(new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(folder)));
    dependencySet.with(diskResourceService);
    serviceLocator = dependencySet.build();
    serviceLocator.startAllServices();
    diskResource = new SizedResourcePoolImpl<>(ResourceType.Core.DISK, 1, MemoryUnit.MB, true);
    spaceIdentifier = diskResourceService.getSharedPersistenceSpaceIdentifier(diskResource);
    sharedPersistence = new StateHolderIdGenerator<>(diskResourceService.getStateRepositoryWithin(spaceIdentifier, "persistent-partition-ids"), String.class);
  }
  @Test
  public void test() throws Exception {
    Set<String> namesToMap = getNames();
    map(namesToMap);
    diskResourceService.releasePersistenceSpaceIdentifier(spaceIdentifier);
    serviceLocator.stopAllServices();

    serviceLocator.startAllServices();
    spaceIdentifier = diskResourceService.getSharedPersistenceSpaceIdentifier(diskResource);
    namesToMap.addAll(getNames());
    map(namesToMap);
    diskResourceService.releasePersistenceSpaceIdentifier(spaceIdentifier);
    serviceLocator.stopAllServices();

    serviceLocator.startAllServices();
    spaceIdentifier = diskResourceService.getSharedPersistenceSpaceIdentifier(diskResource);
    namesToMap.addAll(getNames());
    map(namesToMap);
    diskResourceService.releasePersistenceSpaceIdentifier(spaceIdentifier);
    serviceLocator.stopAllServices();
  }

  private void map(Set<String> names) throws InterruptedException {
    Map<Integer, String> t1Results = new HashMap<>();
    Map<Integer, String> t2Results = new HashMap<>();
    Map<Integer, String> t3Results = new HashMap<>();
    Thread t1 = new Thread(() -> names.forEach(name -> t1Results.put(sharedPersistence.map(name), name)));
    Thread t2 = new Thread(() -> names.forEach(name -> t2Results.put(sharedPersistence.map(name), name)));
    Thread t3 = new Thread(() -> names.forEach(name -> t3Results.put(sharedPersistence.map(name), name)));
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    assertThat(t1Results, is(t2Results));
    assertThat(t1Results, is(t3Results));
    assertThat(t2Results, is(t3Results));
  }

  private Set<String> getNames() {
    Set<String> names = new HashSet<>();
    for (int i = 0; i < 200; i++) {
      names.add(UUID.randomUUID().toString());
    }
    return names;
  }
}
