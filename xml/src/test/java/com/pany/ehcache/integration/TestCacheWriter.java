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

import org.ehcache.spi.writer.CacheWriter;

import java.util.Map;
import java.util.Set;

/**
 * @author Alex Snaps
 */
public class TestCacheWriter implements CacheWriter<Number, String> {

  public static Number lastWrittenKey;

  @Override
  public void write(final Number key, final String value) throws Exception {
    lastWrittenKey = key;
  }

  @Override
  public void writeAll(final Iterable<? extends Map.Entry<? extends Number, ? extends String>> entries) throws Exception {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void delete(final Number key) throws Exception {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void deleteAll(final Iterable<? extends Number> keys) throws Exception {
    throw new UnsupportedOperationException("Implement me!");
  }
}
