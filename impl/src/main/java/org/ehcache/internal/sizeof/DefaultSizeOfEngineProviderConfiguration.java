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

package org.ehcache.internal.sizeof;

import org.ehcache.sizeof.SizeOfEngineProviderConfiguration;
import org.ehcache.spi.sizeof.SizeOfEngineProvider;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineProviderConfiguration implements SizeOfEngineProviderConfiguration {

  private final long maxDepth;
  private final long maxSize;

  public DefaultSizeOfEngineProviderConfiguration(long maxDepth, long maxSize) {
    if(maxDepth <= 0 || maxSize <= 0) {
      throw new IllegalArgumentException("SizeOfEngine cannot take non-positive arguments.");
    }
    this.maxDepth = maxDepth;
    this.maxSize = maxSize;
  }

  @Override
  public Class<SizeOfEngineProvider> getServiceType() {
    return SizeOfEngineProvider.class;
  }

  public long getMaxDepth() {
    return maxDepth;
  }

  public long getMaxSize() {
    return maxSize;
  }



}
