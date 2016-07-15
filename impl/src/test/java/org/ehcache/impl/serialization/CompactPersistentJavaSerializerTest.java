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

import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * CompactPersistentJavaSerializerTest
 */
public class CompactPersistentJavaSerializerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testProperlyInitializesEncodingIndexOnLoad() throws Exception {
    final File folder = temporaryFolder.newFolder("test-cpjs");
    FileBasedPersistenceContext persistenceContext = new FileBasedPersistenceContext() {
      @Override
      public File getDirectory() {
        return folder;
      }
    };

    CompactPersistentJavaSerializer serializer = new CompactPersistentJavaSerializer(getClass().getClassLoader(), persistenceContext);
    ByteBuffer integerBytes = serializer.serialize(10);
    serializer.close();

    serializer = new CompactPersistentJavaSerializer(getClass().getClassLoader(), persistenceContext);
    ByteBuffer longBytes = serializer.serialize(42L);

    assertThat((Integer) serializer.read(integerBytes), is(10));
    assertThat((Long) serializer.read(longBytes), is(42L));
  }

}