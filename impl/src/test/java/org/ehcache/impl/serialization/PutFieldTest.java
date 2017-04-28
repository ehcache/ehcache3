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

import org.ehcache.spi.serialization.StatefulSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
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
public class PutFieldTest {

  @Test
  public void testWithAllPrimitivesAndString() throws Exception {
    @SuppressWarnings("unchecked")
    StatefulSerializer<Serializable> s = new CompactJavaSerializer(null);
    s.init(new TransientStateRepository());

    ClassLoader loaderA = createClassNameRewritingLoader(Foo_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Foo_A.class)).newInstance();
    ByteBuffer encodedA = s.serialize(a);

    pushTccl(Foo.class.getClassLoader());
    try {
      Foo foo = (Foo) s.read(encodedA);

      Assert.assertEquals(true, foo.z);
      Assert.assertEquals(5, foo.b);
      Assert.assertEquals('5', foo.c);
      Assert.assertEquals(5, foo.s);
      Assert.assertEquals(5, foo.i);
      Assert.assertEquals(5, foo.j);
      Assert.assertEquals(5, foo.f, 0.0f);
      Assert.assertEquals(5, foo.d, 0.0);
      Assert.assertEquals("5", foo.str);
    } finally {
      popTccl();
    }
  }

  @Test
  public void testWithTwoStrings() throws Exception {
    @SuppressWarnings("unchecked")
    StatefulSerializer<Serializable> s = new CompactJavaSerializer(null);
    s.init(new TransientStateRepository());

    ClassLoader loaderA = createClassNameRewritingLoader(Bar_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Bar_A.class)).newInstance();
    ByteBuffer encodedA = s.serialize(a);

    pushTccl(Bar.class.getClassLoader());
    try {
      Bar bar = (Bar) s.read(encodedA);

      Assert.assertEquals("qwerty", bar.s1);
      Assert.assertEquals("asdfg", bar.s2);
    } finally {
      popTccl();
    }
  }

  public static class Foo_A implements Serializable {

    private static final long serialVersionUID = 0L;
    private static final ObjectStreamField[] serialPersistentFields = {
      new ObjectStreamField("z", boolean.class),
      new ObjectStreamField("b", byte.class),
      new ObjectStreamField("c", char.class),
      new ObjectStreamField("s", short.class),
      new ObjectStreamField("i", int.class),
      new ObjectStreamField("j", long.class),
      new ObjectStreamField("f", float.class),
      new ObjectStreamField("d", double.class),
      new ObjectStreamField("str", String.class),};

    private void writeObject(ObjectOutputStream out) throws IOException {
      ObjectOutputStream.PutField fields = out.putFields();
      fields.put("z", true);
      fields.put("b", (byte) 5);
      fields.put("c", '5');
      fields.put("s", (short) 5);
      fields.put("i", 5);
      fields.put("j", 5l);
      fields.put("f", 5.0f);
      fields.put("d", 5.0);
      fields.put("str", "5");
      out.writeFields();
    }
  }

  public static class Foo implements Serializable {

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
  }

  public static class Bar_A implements Serializable {

    private static final long serialVersionUID = 0L;
    private static final ObjectStreamField[] serialPersistentFields = {
      new ObjectStreamField("s1", String.class),
      new ObjectStreamField("s2", String.class)
    };

    private void writeObject(ObjectOutputStream out) throws IOException {
      ObjectOutputStream.PutField fields = out.putFields();
      fields.put("s1", "qwerty");
      fields.put("s2", "asdfg");
      out.writeFields();
    }
  }

  public static class Bar implements Serializable {

    private static final long serialVersionUID = 0L;
    String s1, s2;
  }
}
