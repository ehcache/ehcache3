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

import org.ehcache.spi.persistence.StateRepository;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CompactJavaSerializerTest {

  @Test
  public void testStateHolderFailureRereadBehavior() throws ClassNotFoundException {
    ConcurrentMap<Serializable, Serializable> stateMap = spy(new ConcurrentHashMap<Serializable, Serializable>());
    StateRepository stateRepository = mock(StateRepository.class);
    when(stateRepository.getPersistentConcurrentMap("CompactJavaSerializer-ObjectStreamClassIndex")).thenReturn(stateMap);

    final AtomicBoolean failing = new AtomicBoolean();
    Answer<Object> optionalFailure = new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        try {
          return invocation.callRealMethod();
        } finally {
          if (failing.get()) {
            throw new RuntimeException();
          }
        }
      }
    };

    doAnswer(optionalFailure).when(stateMap).entrySet();
    doAnswer(optionalFailure).when(stateMap).get(any());
    doAnswer(optionalFailure).when(stateMap).putIfAbsent(any(Serializable.class), any(Serializable.class));

    CompactJavaSerializer<Date> serializerA = new CompactJavaSerializer<Date>(getClass().getClassLoader(), stateRepository);

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

    assertThat(stateMap.size(), is(1));

    CompactJavaSerializer<Date> serializerB = new CompactJavaSerializer<Date>(getClass().getClassLoader(), stateRepository);
    assertThat(serializerB.read(serialized), is(object));
  }
}
