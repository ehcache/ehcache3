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

package org.ehcache.impl.config.copy;

import org.ehcache.impl.copy.IdentityCopier;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;

public class DefaultCopyProviderConfigurationTest {

  @Test @SuppressWarnings({"unchecked", "rawtypes"})
  public void testDerivedConfigurationDetachesCorrectly() {
    DefaultCopyProviderConfiguration configuration = new DefaultCopyProviderConfiguration();
    configuration.addCopierFor(String.class, (Class) IdentityCopier.class);

    DefaultCopyProviderConfiguration derived = configuration.build(configuration.derive());

    assertThat(derived, is(not(sameInstance(configuration))));
    assertThat(derived.getDefaults().get(String.class).getClazz(), sameInstance(IdentityCopier.class));
  }
}
