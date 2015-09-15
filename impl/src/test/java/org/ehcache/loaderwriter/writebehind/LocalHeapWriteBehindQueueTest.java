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

package org.ehcache.loaderwriter.writebehind;

import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.writebehind.WriteBehindConfigurationBuilder.newWriteBehindConfiguration;
import static org.junit.Assert.fail;

/**
 * LocalHeapWriteBehindQueueTest
 */
public class LocalHeapWriteBehindQueueTest {

  @Test
  public void testLastWriteBlockedOnFullQueue() throws InterruptedException {
    final LocalHeapWriteBehindQueue<String, String> writeBehindQueue = new LocalHeapWriteBehindQueue<String, String>(newWriteBehindConfiguration()
        .queueSize(1)
        .build(), new BlockingLoaderWriter());

    writeBehindQueue.start();

    writeBehindQueue.write("a", "a");

    new Thread(new Runnable() {
      @Override
      public void run() {
        writeBehindQueue.write("b", "b");
      }
    }).start();

    new Thread(new Runnable() {
      @Override
      public void run() {
        writeBehindQueue.write("c", "c");
      }
    }).start();

    BlockingLoaderWriter.firstLatch.countDown();

    boolean await = BlockingLoaderWriter.secondLatch.await(10, TimeUnit.SECONDS);
    if (!await) {
      fail("write still stuck after 10 seconds");
    }
  }

  private static class BlockingLoaderWriter implements CacheLoaderWriter<String, String> {

    static CountDownLatch firstLatch = new CountDownLatch(1);
    static CountDownLatch secondLatch = new CountDownLatch(2);

    @Override
    public String load(String key) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public Map<String, String> loadAll(Iterable<? extends String> keys) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void write(String key, String value) throws Exception {
      if (key.equals("a")) {
        firstLatch.await();
      } else {
        secondLatch.countDown();
      }

    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends String, ? extends String>> entries) throws BulkCacheWritingException, Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void delete(String key) throws Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends String> keys) throws BulkCacheWritingException, Exception {
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }
}