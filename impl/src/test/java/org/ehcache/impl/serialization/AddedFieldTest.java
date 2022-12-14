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
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
public class AddedFieldTest {

  @Test
  public void addingSerializableField() throws Exception {
    StatefulSerializer<Serializable> serializer = new CompactJavaSerializer<>(null);
    serializer.init(new TransientStateRepository());

    ClassLoader loaderA = createClassNameRewritingLoader(A_write.class, IncompatibleSerializable_write.class, Serializable_write.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_write.class)).newInstance();
    ByteBuffer encodedA = serializer.serialize(a);

    pushTccl(createClassNameRewritingLoader(A_read.class, IncompatibleSerializable_read.class));
    try {
      Serializable out = serializer.read(encodedA);
      Assert.assertThat(out.getClass().getField("bar").getInt(out), Is.is(4));
    } finally {
      popTccl();
    }
  }

  @Test
  public void addingExternalizableField() throws Exception {
    StatefulSerializer<Serializable> serializer = new CompactJavaSerializer<>(null);
    serializer.init(new TransientStateRepository());

    ClassLoader loaderA = createClassNameRewritingLoader(B_write.class, Externalizable_write.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(B_write.class)).newInstance();
    ByteBuffer encodedA = serializer.serialize(a);

    pushTccl(createClassNameRewritingLoader(B_read.class));
    try {
      Serializable out = serializer.read(encodedA);
      Assert.assertThat(out.getClass().getField("bar").getInt(out), Is.is(4));
    } finally {
      popTccl();
    }
  }

  public static class Serializable_write implements Serializable {

    private static final long serialVersionUID = 1L;

    int k;

    Serializable_write(int value) {
      k = value;
    }
  };

  public static class IncompatibleSerializable_write implements Serializable {

    private static final long serialVersionUID = 4L;

    int x = 5;
  };

  public static class IncompatibleSerializable_read implements Serializable {

    private static final long serialVersionUID = 4L;

    int x = 5;
  };

  public static class A_write implements Serializable {
    // Version 1.1 of class A.  Added superclass NewSerializableSuper.

    private static final long serialVersionUID = 1L;
    Serializable_write newFieldOfMissingType;
    IncompatibleSerializable_write newFieldOfIncompatibleType;
    int bar;

    public A_write() {
      newFieldOfMissingType = new Serializable_write(23);
      newFieldOfIncompatibleType = new IncompatibleSerializable_write();
      bar = 4;
    }
  }

  public static class A_read implements Serializable {

    private static final long serialVersionUID = 1L;
    public int bar;
  }

  public static class Externalizable_write implements Externalizable {

    private static final long serialVersionUID = 1L;

    byte l;

    public Externalizable_write() {
      l = 0;
    }

    public Externalizable_write(byte value) {
      l = value;
    }

    @Override
    public void readExternal(ObjectInput s)
            throws IOException, ClassNotFoundException {
      l = s.readByte();
    }

    @Override
    public void writeExternal(ObjectOutput s) throws IOException {
      s.writeByte(l);
    }
  }

  public static class B_write implements Serializable {
    // Version 1.1 of class A.  Added superclass NewSerializableSuper.

    private static final long serialVersionUID = 1L;
    Externalizable_write foo;
    int bar;

    public B_write() {
      bar = 4;
      foo = new Externalizable_write((byte) 66);
    }
  }

  public static class B_read implements Serializable {
    // Version 1.0 of class A.

    private static final long serialVersionUID = 1L;
    public int bar;
  }
}
