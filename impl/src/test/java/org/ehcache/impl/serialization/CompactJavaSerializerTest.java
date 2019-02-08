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
package org.ehcache.impl.serialization;

import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CompactJavaSerializerTest {

  @Test
  public void testStateHolderFailureRereadBehavior() throws ClassNotFoundException {
    StateHolder<Integer, ObjectStreamClass> stateMap = spy(new TransientStateHolder<>());
    StateRepository stateRepository = mock(StateRepository.class);
    when(stateRepository.getPersistentStateHolder(eq("CompactJavaSerializer-ObjectStreamClassIndex"), eq(Integer.class), eq(ObjectStreamClass.class), any(), any())).thenReturn(stateMap);

    AtomicBoolean failing = new AtomicBoolean();
    Answer<Object> optionalFailure = invocation -> {
      try {
        return invocation.callRealMethod();
      } finally {
        if (failing.get()) {
          throw new RuntimeException();
        }
      }
    };

    doAnswer(optionalFailure).when(stateMap).entrySet();
    doAnswer(optionalFailure).when(stateMap).get(any());
    doAnswer(optionalFailure).when(stateMap).putIfAbsent(any(), any());

    CompactJavaSerializer<Date> serializerA = new CompactJavaSerializer<Date>(getClass().getClassLoader());
    serializerA.init(stateRepository);

    Date object = new Date();

    failing.set(true);
    try {
      serializerA.serialize(object);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      //expected
    }

    failing.set(false);
    ByteBuffer serialized = serializerA.serialize(object);
    assertThat(serializerA.read(serialized), is(object));

    assertThat(stateMap.entrySet(), hasSize(1));

    CompactJavaSerializer<Date> serializerB = new CompactJavaSerializer<Date>(getClass().getClassLoader());
    serializerB.init(stateRepository);

    assertThat(serializerB.read(serialized), is(object));
  }
}
