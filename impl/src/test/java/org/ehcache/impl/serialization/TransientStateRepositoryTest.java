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

package org.ehcache.impl.serialization;

import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * TransientStateRepositoryTest
 */
public class TransientStateRepositoryTest {

  @Test
  public void testRemembersCreatedMaps() throws Exception {
    TransientStateRepository repository = new TransientStateRepository();
    ConcurrentMap<Long, String> test = repository.getPersistentConcurrentMap("test", Long.class, String.class);
    test.put(42L, "Again??");

    test = repository.getPersistentConcurrentMap("test", Long.class, String.class);
    assertThat(test.get(42L), is("Again??"));
  }

}