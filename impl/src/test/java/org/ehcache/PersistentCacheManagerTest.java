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

package org.ehcache;

import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.exceptions.StateTransitionException;
import org.junit.Test;

import java.io.File;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Alex Snaps
 */
public class PersistentCacheManagerTest {

  @Test
  public void testInitializesLocalPersistenceService() {
    final File rootDirectory = mock(File.class);
    when(rootDirectory.exists()).thenReturn(true);
    when(rootDirectory.getAbsolutePath()).thenReturn("CRAP!");
    try {
      newCacheManagerBuilder().with(new PersistenceConfiguration(rootDirectory)).build(true);
    } catch (Exception e) {
      assertThat(e, instanceOf(StateTransitionException.class));
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
      assertThat(e.getCause().getMessage().endsWith("CRAP!"), is(true));
    }
  }
}
