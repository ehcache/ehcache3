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
package org.ehcache.integration;

import org.ehcache.UserManagedCache;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.junit.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anthony Dahanne
 * Simple test to make sure eviction is happening when we specify a capacity
 */
public class UserManagedCacheEvictionTest {

  @Test
  public void test_eviction() throws Exception {
    UserManagedCache<Number, String> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(Number.class, String.class)
        .withResourcePools(newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES))
        .build(true);
    assertThat(cache.getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(),
        equalTo(1L));

    // we put 3 elements, but there's only capacity for 1
    for (int i = 0; i < 3; i++) {
      cache.putIfAbsent(i, "" + i);
    }

    // we must find at most 1 non empty value
    int nullValuesFound = 0;
    for (int i = 0; i < 3; i++) {
      String retrievedValue = cache.get(i);
      if (retrievedValue == null) {
        nullValuesFound ++;
      }
    }
    assertThat("The capacity of the store is 1, and we found more than 1 non empty value in it !", nullValuesFound, is(2));
  }

}
