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

package org.ehcache.clustered.server.state;

import org.junit.Test;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class InvalidationTrackerManagerImplTest {

  @Test
  public void getInvalidationTracker() throws Exception {
    InvalidationTrackerManagerImpl manager = new InvalidationTrackerManagerImpl();
    manager.addInvalidationTracker("foo");
    assertThat(manager.getInvalidationTracker("foo"), notNullValue());
  }

  @Test
  public void removeInvalidationTracker() throws Exception {
    InvalidationTrackerManagerImpl manager = new InvalidationTrackerManagerImpl();
    manager.addInvalidationTracker("foo");
    assertThat(manager.getInvalidationTracker("foo"), notNullValue());
    manager.removeInvalidationTracker("foo");
    assertThat(manager.getInvalidationTracker("foo"), nullValue());
  }

  @Test
  public void clear() throws Exception {
    InvalidationTrackerManagerImpl manager = new InvalidationTrackerManagerImpl();
    manager.addInvalidationTracker("foo");
    assertThat(manager.getInvalidationTracker("foo"), notNullValue());
    manager.clear();
    assertThat(manager.getInvalidationTracker("foo"), nullValue());
  }

}
