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
package org.ehcache.sizeof;

import org.ehcache.spi.service.Service;

/**
 * @author Abhilash
 *
 */
public interface SizeOfEngineProvider extends Service {

  /**
   * Creates a {@link SizeOfEngine} which will size objects 
   * with maximum depth and maximum size 
   * 
   * @param maxDepth No. of objects traversed as part of Object graph
   * @param maxSize  The max size till the object graph will be traversed 
   * @return {@link SizeOfEngine} with provided maxDeptha an maxSize
   */
  SizeOfEngine createSizeOfEngine(long maxDepth, long maxSize);
}
