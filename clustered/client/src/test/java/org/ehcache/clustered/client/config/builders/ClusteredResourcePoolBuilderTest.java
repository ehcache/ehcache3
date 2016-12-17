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

package org.ehcache.clustered.client.config.builders;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;

public class ClusteredResourcePoolBuilderTest {

  @Test
  public void dedicated2Arg() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.clusteredDedicated(16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is(nullValue()));
  }

  @Test
  public void dedicated2ArgUnitNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.clusteredDedicated(16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void dedicated3Arg() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.clusteredDedicated("resourceId", 16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is("resourceId"));
  }

  @Test
  public void dedicated3ArgFromNull() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.clusteredDedicated(null, 16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(DedicatedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.DEDICATED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((DedicatedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((DedicatedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((DedicatedClusteredResourcePool)pool).getFromResource(), is(nullValue()));
  }

  @Test
  public void dedicated3ArgUnitNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.clusteredDedicated("resourceId", 16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void shared() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.clusteredShared("resourceId");
    assertThat(pool, is(instanceOf(SharedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.SHARED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((SharedClusteredResourcePool)pool).getSharedResourcePool(), is("resourceId"));
  }

  @Test
  public void sharedSharedResourceNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.clusteredShared(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

  }
}