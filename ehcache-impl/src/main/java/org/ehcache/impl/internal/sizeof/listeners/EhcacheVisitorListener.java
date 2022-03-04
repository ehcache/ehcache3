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
package org.ehcache.impl.internal.sizeof.listeners;

import org.ehcache.impl.internal.sizeof.listeners.exceptions.VisitorListenerException;
import org.ehcache.sizeof.VisitorListener;

/**
 * @author Abhilash
 *
 */

public class EhcacheVisitorListener implements VisitorListener {

  private final long maxObjectGraphSize;
  private final long maxObjectSize;
  private long currentDepth;
  private long currentSize;

  public EhcacheVisitorListener(long maxObjectGraphSize, long maxObjectSize) {
    this.maxObjectGraphSize = maxObjectGraphSize;
    this.maxObjectSize = maxObjectSize;
  }

  @Override
  public void visited(Object object, long size) {
    if((currentDepth += 1) > maxObjectGraphSize) {
      throw new VisitorListenerException("Max Object Graph Size reached for the object : "+ object);
    }
    if((currentSize += size) > maxObjectSize) {
      throw new VisitorListenerException("Max Object Size reached for the object : "+ object);
    }
  }

}
