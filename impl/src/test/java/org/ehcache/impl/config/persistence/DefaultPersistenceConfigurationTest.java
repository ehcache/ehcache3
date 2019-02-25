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

package org.ehcache.impl.config.persistence;

import org.junit.Test;

import java.io.File;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;

public class DefaultPersistenceConfigurationTest {

  @Test
  public void testDeriveDetachesCorrectly() {
    DefaultPersistenceConfiguration configuration = new DefaultPersistenceConfiguration(new File("foo"));
    DefaultPersistenceConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived, is(not(sameInstance(configuration))));
    assertThat(derived.getRootDirectory(), is(configuration.getRootDirectory()));
  }
}
