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
package org.ehcache.internal.sizeof.listeners;

import org.ehcache.internal.sizeof.listeners.exceptions.VisitorListenerException;
import org.ehcache.sizeof.VisitorListener;

/**
 * @author Abhilash
 *
 */

public class EhcacheVisitorListener implements VisitorListener {
  
  private final long maxDepth;
  private final long maxSize;
  private long currentDepth;
  private long currentSize;
  
  public EhcacheVisitorListener(long maxDepth, long maxSize) {
    this.maxDepth = maxDepth;
    this.maxSize = maxSize;
  }

  @Override
  public void visited(Object object, long size) {
    if((currentDepth += 1) > maxDepth) {
      throw new VisitorListenerException("Max Depth reached for the object : "+ object);
    }
    if((currentSize += size) > maxSize) {
      throw new VisitorListenerException("Max Size reached for the object : "+ object);
    }
    
  }

}
