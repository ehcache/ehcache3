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
package org.ehcache.clustered.common.messages;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage.ServerStoreOp;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EhcacheEntityMessageOpCodeTest {

  @Test
  public void testServerStoreMessageOpCode() {
    // Asserts server store message opcodes
    ServerStoreOp[] storeOps = ServerStoreOp.values();
    byte base = 10;
    for (byte i = 0 ; i < storeOps.length; i++) {
      assertThat(storeOps[i].getStoreOpCode(), is((byte)(base + i)));
    }

  }

  @Test
  public void testLifeCycleMessageOpCode() {
    LifeCycleMessageFactory messageFactory = new LifeCycleMessageFactory();

    ServerSideConfiguration serverSideConfiguration = Mockito.mock(ServerSideConfiguration.class);
    ServerStoreConfiguration serverStoreConfiguration = Mockito.mock(ServerStoreConfiguration.class);

    assertThat(messageFactory.configureStoreManager(serverSideConfiguration).getOpCode(), is((byte)1));
    assertThat(messageFactory.validateStoreManager(serverSideConfiguration).getOpCode(), is((byte)1));

    assertThat(messageFactory.createServerStore("test", serverStoreConfiguration).getOpCode(), is((byte)1));
    assertThat(messageFactory.validateServerStore("test", serverStoreConfiguration).getOpCode(), is((byte)1));
    assertThat(messageFactory.destroyServerStore("test").getOpCode(), is((byte)1));
    assertThat(messageFactory.releaseServerStore("test").getOpCode(), is((byte)1));

  }

}

