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

import org.ehcache.spi.serialization.Serializer;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.ehcache.impl.serialization.SerializerTestUtilities.createClassNameRewritingLoader;
import static org.ehcache.impl.serialization.SerializerTestUtilities.newClassName;
import static org.ehcache.impl.serialization.SerializerTestUtilities.popTccl;
import static org.ehcache.impl.serialization.SerializerTestUtilities.pushTccl;

/**
 *
 * @author cdennis
 */
public class AddedSuperClassTest {

  @Test
  public void testAddedSuperClass() throws Exception {
    Serializer<Serializable> serializer = new CompactJavaSerializer(null);

    ClassLoader loaderA = createClassNameRewritingLoader(A_2.class, AddedSuperClass_Hidden.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_2.class)).newInstance();
    ByteBuffer encodedA = serializer.serialize(a);

    pushTccl(createClassNameRewritingLoader(A_1.class));
    try {
      serializer.read(encodedA);
    } finally {
      popTccl();
    }
  }

  @Test
  public void testAddedSuperClassNotHidden() throws Exception {
    Serializer<Serializable> serializer = new CompactJavaSerializer(null);

    ClassLoader loaderA = createClassNameRewritingLoader(A_2.class, AddedSuperClass_Hidden.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_2.class)).newInstance();
    ByteBuffer encodedA = serializer.serialize(a);

    pushTccl(createClassNameRewritingLoader(A_1.class, AddedSuperClass_Hidden.class));
    try {
      serializer.read(encodedA);
    } finally {
      popTccl();
    }
  }

  public static class AddedSuperClass_Hidden implements Serializable {
    int field;
  }

  public static class A_2 extends AddedSuperClass_Hidden  {
    private static final long serialVersionUID = 1L;
  }

  public static class A_1 implements Serializable {
    private static final long serialVersionUID = 1L;
  }

}
