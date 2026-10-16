/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2026
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

package org.ehcache.impl.internal.concurrent;

import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.Remapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * JDK 24+ warns on, and later JDKs reject, the {@code sun.misc.Unsafe} memory-access methods.
 */
public class NoUnsafeUsageTest {

  @Test
  public void concurrentHashMapDoesNotReferenceUnsafe() throws IOException {
    for (Class<?> type : ConcurrentHashMap.class.getNestMembers()) {
      assertDoesNotReferenceUnsafe(type);
    }
  }

  @Test
  public void threadLocalRandomUtilDoesNotReferenceUnsafe() throws IOException {
    assertDoesNotReferenceUnsafe(ThreadLocalRandomUtil.class);
  }

  private static void assertDoesNotReferenceUnsafe(Class<?> type) throws IOException {
    assertThat(type.getName(), referencedTypes(type), not(hasItem("sun/misc/Unsafe")));
  }

  private static Set<String> referencedTypes(Class<?> type) throws IOException {
    Set<String> types = new HashSet<>();
    try (InputStream classFile = type.getResourceAsStream('/' + type.getName().replace('.', '/') + ".class")) {
      new ClassReader(classFile).accept(new ClassRemapper(new ClassWriter(0), new Remapper() {
        @Override
        public String map(String internalName) {
          types.add(internalName);
          return internalName;
        }
      }), 0);
    }
    return types;
  }
}
