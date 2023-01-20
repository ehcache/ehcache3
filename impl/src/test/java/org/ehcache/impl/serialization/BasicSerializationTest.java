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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Random;

import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.spi.serialization.Serializer;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.IsSame;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class BasicSerializationTest {

  @Test
  public void testSimpleObject() throws ClassNotFoundException {
    Serializer<Serializable> test = new CompactJavaSerializer(null);

    String input = "";
    String result = (String) test.read(test.serialize(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);
  }

  @Test
  public void testComplexObject() throws ClassNotFoundException {
    Serializer<Serializable> test = new CompactJavaSerializer(null);

    HashMap<Integer, String> input = new HashMap<Integer, String>();
    input.put(1, "one");
    input.put(2, "two");
    input.put(3, "three");

    HashMap<?, ?> result = (HashMap<?, ?>) test.read(test.serialize(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);

  }

  private static final Class[] PRIMITIVE_CLASSES = new Class[] {
     boolean.class, byte.class, char.class, short.class,
     int.class, long.class, float.class, double.class, void.class
  };

  @Test
  public void testPrimitiveClasses() throws ClassNotFoundException {
    Serializer<Serializable> s = new CompactJavaSerializer(null);

    Class[] out = (Class[]) s.read(s.serialize(PRIMITIVE_CLASSES));

    Assert.assertThat(out, IsNot.not(IsSame.sameInstance(PRIMITIVE_CLASSES)));
    Assert.assertThat(out, IsEqual.equalTo(PRIMITIVE_CLASSES));
  }

  @Test
  public void testProxyInstance() throws ClassNotFoundException {
    Random rand = new Random();
    int foo = rand.nextInt();
    float bar = rand.nextFloat();

    Serializer<Serializable> s = new CompactJavaSerializer(null);

    Object proxy = s.read(s.serialize((Serializable) Proxy.newProxyInstance(BasicSerializationTest.class.getClassLoader(), new Class[]{Foo.class, Bar.class}, new Handler(foo, bar))));

    Assert.assertThat(((Foo) proxy).foo(), Is.is(foo));
    Assert.assertThat(((Bar) proxy).bar(), Is.is(bar));
  }

  interface Foo {
    int foo();
  }

  interface Bar {
    float bar();
  }

  static class Handler implements InvocationHandler, Serializable {

    static Method fooMethod, barMethod;

    static {
      try {
        fooMethod = Foo.class.getDeclaredMethod("foo", new Class[0]);
        barMethod = Bar.class.getDeclaredMethod("bar", new Class[0]);
      } catch (NoSuchMethodException ex) {
        throw new Error();
      }
    }
    int foo;
    float bar;

    Handler(int foo, float bar) {
      this.foo = foo;
      this.bar = bar;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
      if (method.equals(fooMethod)) {
        return foo;
      } else if (method.equals(barMethod)) {
        return bar;
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }
}
