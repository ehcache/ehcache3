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
package com.pany.ehcache.integration;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

public class ThreadRememberingLoaderWriter implements CacheLoaderWriter<String, String> {
  public static volatile Semaphore USED = new Semaphore(0);
  public static volatile Thread LAST_SEEN_THREAD;

  @Override
  public String load(String key) {
    return null;
  }

  @Override
  public Map<String, String> loadAll(Iterable<? extends String> keys) {
    return Collections.emptyMap();
  }

  @Override
  public void write(String key, String value) {
    LAST_SEEN_THREAD = Thread.currentThread();
    USED.release();
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends String, ? extends String>> entries) {
    LAST_SEEN_THREAD = Thread.currentThread();
    USED.release();
  }

  @Override
  public void delete(String key) {
    LAST_SEEN_THREAD = Thread.currentThread();
    USED.release();
  }

  @Override
  public void deleteAll(Iterable<? extends String> keys) {
    LAST_SEEN_THREAD = Thread.currentThread();
    USED.release();
  }

}
