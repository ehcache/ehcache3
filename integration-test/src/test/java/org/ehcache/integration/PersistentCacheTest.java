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
package org.ehcache.integration;

import org.ehcache.CacheManagerBuilder;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.integration.util.JavaExec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class PersistentCacheTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPersistentCachesColliding() throws Exception {
    PersistentCacheManager cm = CacheManagerBuilder.newCacheManagerBuilder()
            .with(new PersistenceConfiguration(new File(temporaryFolder.getRoot(), "testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass"))).build();
    try {
      CacheManagerBuilder.newCacheManagerBuilder().with(new PersistenceConfiguration(new File(temporaryFolder.getRoot(), "testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass"))).build().close();
      Assert.fail("Expected StateTransitionException");
    } catch (StateTransitionException e) {
      assertThat(e.getCause().getMessage(), containsString("Persistence directory already locked by this process"));
      assertThat(e.getCause().getCause(), instanceOf(OverlappingFileLockException.class));
    } finally {
      cm.close();
    }
  }

  @Test
  public void testPersistentCachesCollidingCrossProcess() throws Exception {
    File folder = new File(temporaryFolder.getRoot(), "testRecoverPersistentCacheSucceedsWhenConfiguringArrayClass");
    File ping = new File(folder, "ping");
    File pong = new File(folder, "pong");

    Future<Integer> external = JavaExec.exec(Locker.class, folder.getAbsolutePath());
    while(!ping.exists());
    try {
      CacheManagerBuilder.newCacheManagerBuilder().with(new PersistenceConfiguration(folder)).build().close();
      Assert.fail("Expected StateTransitionException");
    } catch (StateTransitionException e) {
      assertThat(e.getCause().getMessage(), containsString("Persistence directory already locked by another process"));
    } finally {
      pong.createNewFile();
      assertThat(external.get(), is(0));
    }
  }

  public static final class Locker {

    public static void main(String[] args) throws Exception {
      File folder = new File(args[0]);
      File ping = new File(folder, "ping");
      File pong = new File(folder, "pong");

      PersistentCacheManager cm = CacheManagerBuilder.newCacheManagerBuilder()
              .with(new PersistenceConfiguration(folder)).build();
      try {
        ping.createNewFile();
        long bailout = System.nanoTime() + SECONDS.toNanos(30);
        while (System.nanoTime() < bailout && !pong.exists());
      } finally {
        cm.close();
      }
    }
  }
}
