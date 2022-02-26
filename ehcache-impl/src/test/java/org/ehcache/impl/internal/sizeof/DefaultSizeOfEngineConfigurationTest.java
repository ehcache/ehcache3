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

package org.ehcache.impl.internal.sizeof;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.fail;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.junit.Test;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineConfigurationTest {

  @Test
  public void testIllegalMaxObjectSizeArgument() {
    try {
      new DefaultSizeOfEngineConfiguration(0, MemoryUnit.B, 1l);
      fail();
    } catch (Exception illegalArgument) {
      assertThat(illegalArgument, instanceOf(IllegalArgumentException.class));
      assertThat(illegalArgument.getMessage(), equalTo("ObjectGraphSize/ObjectSize can only accept positive values."));
    }
  }

  @Test
  public void testIllegalMaxObjectGraphSizeArgument() {
    try {
      new DefaultSizeOfEngineConfiguration(1l, MemoryUnit.B, 0);
      fail();
    } catch (Exception illegalArgument) {
      assertThat(illegalArgument, instanceOf(IllegalArgumentException.class));
      assertThat(illegalArgument.getMessage(), equalTo("ObjectGraphSize/ObjectSize can only accept positive values."));
    }
  }

  @Test
  public void testValidArguments() {
    DefaultSizeOfEngineConfiguration configuration = new DefaultSizeOfEngineConfiguration(10l, MemoryUnit.B, 10l);
    assertThat(configuration.getMaxObjectGraphSize(), equalTo(10l));
    assertThat(configuration.getMaxObjectSize(), equalTo(10l));
    assertThat(configuration.getUnit(), equalTo(MemoryUnit.B));
  }

  @Test
  public void testDeriveDetachesProperly() {
    DefaultSizeOfEngineConfiguration configuration = new DefaultSizeOfEngineConfiguration(42L, MemoryUnit.MB, 123L);
    DefaultSizeOfEngineConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived, is(not(sameInstance(configuration))));
    assertThat(derived.getMaxObjectSize(), is(configuration.getMaxObjectSize()));
    assertThat(derived.getUnit(), is(configuration.getUnit()));
    assertThat(derived.getMaxObjectGraphSize(), is(configuration.getMaxObjectGraphSize()));
  }
}
