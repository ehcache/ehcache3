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
package org.ehcache.sizeof;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.sizeof.SizeOfEngineProvider;

/**
 * @author Abhilash
 *
 */
public interface SizeOfEngineConfiguration extends ServiceConfiguration<SizeOfEngineProvider> {

  /**
   * No. of objects traversed as part of Object graph
   *
   * @return maximum number of objects traversed by this sizeofengine
   */
  long getMaxDepth();

  /**
   * The max size till the object graph will be traversed
   *
   * @return size limit after which traversal of object graph will return
   */
  long getMaxSize();
}
