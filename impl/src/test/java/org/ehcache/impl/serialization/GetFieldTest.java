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

import java.io.IOException;
import java.io.ObjectInputStream;
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
public class GetFieldTest {

  @Test
  public void testGetField() throws Exception {
    Serializer<Serializable> s = new CompactJavaSerializer(null);

    ClassLoader loaderA = createClassNameRewritingLoader(Foo_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Foo_A.class)).newInstance();
    ByteBuffer encodedA = s.serialize(a);

    pushTccl(createClassNameRewritingLoader(Foo_B.class));
    try {
      s.read(encodedA.duplicate());
    } finally {
      popTccl();
    }

    pushTccl(createClassNameRewritingLoader(Foo_C.class));
    try {
      s.read(encodedA.duplicate());
    } finally {
      popTccl();
    }
  }

  public static class Foo_A implements Serializable {

    private static final long serialVersionUID = 0L;
    boolean z = true;
    byte b = 5;
    char c = '5';
    short s = 5;
    int i = 5;
    long j = 5;
    float f = 5.0f;
    double d = 5.0;
    String str = "5";
  }

  public static class Foo_B implements Serializable {

    private static final long serialVersionUID = 0L;
    int blargh;

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField fields = in.readFields();
      if (!fields.defaulted("blargh")) {
        throw new Error();
      }
      try {
        fields.defaulted("nonexistant");
        throw new Error();
      } catch (IllegalArgumentException ex) {
      }
      if ((fields.get("z", false) != true)
              || (fields.get("b", (byte) 0) != 5)
              || (fields.get("c", '0') != '5')
              || (fields.get("s", (short) 0) != 5)
              || (fields.get("i", 0) != 5)
              || (fields.get("j", 0l) != 5)
              || (fields.get("f", 0.0f) != 5.0f)
              || (fields.get("d", 0.0) != 5.0)
              || (!fields.get("str", null).equals("5"))) {
        throw new Error();
      }
    }
  }

  public static class Foo_C implements Serializable {

    private static final long serialVersionUID = 0L;
    boolean z;
    byte b;
    char c;
    short s;
    int i;
    long j;
    float f;
    double d;
    String str;
    Object extra;

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField fields = in.readFields();
      if ((fields.get("z", false) != true)
              || (fields.get("b", (byte) 0) != 5)
              || (fields.get("c", '0') != '5')
              || (fields.get("s", (short) 0) != 5)
              || (fields.get("i", 0) != 5)
              || (fields.get("j", 0l) != 5)
              || (fields.get("f", 0.0f) != 5.0f)
              || (fields.get("d", 0.0) != 5.0)
              || (!fields.get("str", null).equals("5"))) {
        throw new Error();
      }
    }
  }
}
