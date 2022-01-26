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
package org.ehcache.impl.internal.classes;

import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Ludovic Orban
 */
public class ClassInstanceProviderConfigurationTest {

  @Test
  public void testOrdering() throws Exception {
    ClassInstanceProviderConfiguration<String, ClassInstanceConfiguration<String>> classInstanceProviderFactoryConfig = new ClassInstanceProviderConfiguration<>();

    for (int i = 0; i < 100; i++) {
      classInstanceProviderFactoryConfig.getDefaults().put("" + i, new ClassInstanceConfiguration<String>(String.class));
    }

    Map<String, ClassInstanceConfiguration<String>> defaults = classInstanceProviderFactoryConfig.getDefaults();
    Iterator<String> iterator = defaults.keySet().iterator();
    for (int i = 0; i < defaults.size(); i++) {
      assertThat("" + i, equalTo(iterator.next()));
    }
  }

}
