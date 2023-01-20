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
import org.ehcache.clustered.client.config.FixedClusteredResourcePool;
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

public class ClusteredResourcePoolBuilderTest {

  @Test
  public void fixed2Arg() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.fixed(16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(FixedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.FIXED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((FixedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((FixedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((FixedClusteredResourcePool)pool).getFromResource(), is(nullValue()));
  }

  @Test
  public void fixed2ArgUnitNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.fixed(16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void fixed3Arg() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.fixed("resourceId", 16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(FixedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.FIXED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((FixedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((FixedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((FixedClusteredResourcePool)pool).getFromResource(), is("resourceId"));
  }

  @Test
  public void fixed3ArgFromNull() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.fixed(null, 16, MemoryUnit.GB);
    assertThat(pool, is(instanceOf(FixedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.FIXED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((FixedClusteredResourcePool)pool).getSize(), is(16L));
    assertThat(((FixedClusteredResourcePool)pool).getUnit(), is(MemoryUnit.GB));
    assertThat(((FixedClusteredResourcePool)pool).getFromResource(), is(nullValue()));
  }

  @Test
  public void fixed3ArgUnitNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.fixed("resourceId", 16, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void shared() throws Exception {
    ResourcePool pool = ClusteredResourcePoolBuilder.shared("resourceId");
    assertThat(pool, is(instanceOf(SharedClusteredResourcePool.class)));
    assertThat(pool.getType(), Matchers.<ResourceType>is(ClusteredResourceType.Types.SHARED));
    assertThat(pool.isPersistent(), is(true));
    assertThat(((SharedClusteredResourcePool)pool).getSharedResource(), is("resourceId"));
  }

  @Test
  public void sharedSharedResourceNull() throws Exception {
    try {
      ClusteredResourcePoolBuilder.shared(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

  }
}