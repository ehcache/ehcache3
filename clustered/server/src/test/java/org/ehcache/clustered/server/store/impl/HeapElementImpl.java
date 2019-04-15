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
package org.ehcache.clustered.server.store.impl;

import org.ehcache.clustered.common.internal.store.Element;

import java.nio.ByteBuffer;

/**
 * Implements {@link Element}
 */
public class HeapElementImpl implements Element {

  private final long sequenceNumber;
  private final ByteBuffer data;

  public HeapElementImpl(long sequenceNumber, ByteBuffer data) {
    this.sequenceNumber = sequenceNumber;
    this.data = data;
  }

  public long getSequenceNumber() {
    return this.sequenceNumber;
  }

  @Override
  public ByteBuffer getPayload() {
    return this.data.asReadOnlyBuffer();
  }
}
