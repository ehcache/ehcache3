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
package org.ehcache.management.registry;

import org.junit.Test;

import static junit.framework.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class DefaultManagementRegistryTest {

  @Test
  public void testRegisterOnlyWorksWhenStarted() throws Exception {
    DefaultManagementRegistry defaultManagementRegistry = new DefaultManagementRegistry();

    try {
      defaultManagementRegistry.register(Object.class, new Object());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    defaultManagementRegistry.start(null);

    defaultManagementRegistry.register(Object.class, new Object());

    defaultManagementRegistry.stop();

    try {
      defaultManagementRegistry.register(Object.class, new Object());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testStartingMultipleTimes() throws Exception {
    DefaultManagementRegistry defaultManagementRegistry = new DefaultManagementRegistry();

    try {
      defaultManagementRegistry.register(Object.class, new Object());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    defaultManagementRegistry.start(null);
    defaultManagementRegistry.start(null);
    defaultManagementRegistry.start(null);

    defaultManagementRegistry.register(Object.class, new Object());

    defaultManagementRegistry.stop();
    defaultManagementRegistry.register(Object.class, new Object());
    defaultManagementRegistry.stop();
    defaultManagementRegistry.register(Object.class, new Object());
    defaultManagementRegistry.stop();

    try {
      defaultManagementRegistry.register(Object.class, new Object());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }


    defaultManagementRegistry.stop();
    defaultManagementRegistry.stop();
    defaultManagementRegistry.stop();

    defaultManagementRegistry.start(null);
    defaultManagementRegistry.register(Object.class, new Object());
    defaultManagementRegistry.stop();

  }

  @Test
  public void testRestartingAfterStops() throws Exception {
    DefaultManagementRegistry defaultManagementRegistry = new DefaultManagementRegistry();

    defaultManagementRegistry.start(null);

    defaultManagementRegistry.register(Object.class, new Object());

    defaultManagementRegistry.stop();
    defaultManagementRegistry.stop();
    defaultManagementRegistry.stop();

    try {
      defaultManagementRegistry.register(Object.class, new Object());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    defaultManagementRegistry.start(null);
    defaultManagementRegistry.register(Object.class, new Object());
    defaultManagementRegistry.stop();
  }

}
