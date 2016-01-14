/*
 * 
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

import org.ehcache.sizeof.SizeOfEngine;
import org.ehcache.sizeof.SizeOfEngineProvider;
import org.ehcache.spi.ServiceProvider;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineProvider implements SizeOfEngineProvider {

  @Override
  public void start(ServiceProvider serviceProvider) {
    //no op
  }

  @Override
  public void stop() {
    //no op
  }

  @Override
  public SizeOfEngine createSizeOfEngine(long maxDepth, long maxSize) {
    return new DefaultSizeOfEngine(maxDepth, maxSize);
  }

}
