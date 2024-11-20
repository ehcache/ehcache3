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

import org.ehcache.spi.serialization.StatefulSerializer;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.IsSame;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author cdennis
 */
public class BasicSerializationTest {

  @Test
  public void testSimpleObject() throws ClassNotFoundException {
    StatefulSerializer<Serializable> test = new CompactJavaSerializer<>(null);
    test.init(new TransientStateRepository());

    String input = "";
    String result = (String) test.read(test.serialize(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);
  }

  @Test
  public void testComplexObject() throws ClassNotFoundException {
    StatefulSerializer<Serializable> test = new CompactJavaSerializer<>(null);
    test.init(new TransientStateRepository());

    HashMap<Integer, String> input = new HashMap<>();
    input.put(1, "one");
    input.put(2, "two");
    input.put(3, "three");

    HashMap<?, ?> result = (HashMap<?, ?>) test.read(test.serialize(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);

  }

  private static final Class<?>[] PRIMITIVE_CLASSES = new Class<?>[] {
     boolean.class, byte.class, char.class, short.class,
     int.class, long.class, float.class, double.class, void.class
  };

  @Test
  public void testPrimitiveClasses() throws ClassNotFoundException {
    StatefulSerializer<Serializable> s = new CompactJavaSerializer<>(null);
    s.init(new TransientStateRepository());

    Class<?>[] out = (Class<?>[]) s.read(s.serialize(PRIMITIVE_CLASSES));

    assertThat(out, IsNot.not(IsSame.sameInstance(PRIMITIVE_CLASSES)));
    assertThat(out, IsEqual.equalTo(PRIMITIVE_CLASSES));
  }

  @Test
  public void testProxyInstance() throws ClassNotFoundException {
    Random rand = new Random();
    int foo = rand.nextInt();
    float bar = rand.nextFloat();

    StatefulSerializer<Serializable> s = new CompactJavaSerializer<>(null);
    s.init(new TransientStateRepository());

    Object proxy = s.read(s.serialize((Serializable) Proxy.newProxyInstance(BasicSerializationTest.class.getClassLoader(), new Class<?>[]{Foo.class, Bar.class}, new Handler(foo, bar))));

    assertThat(((Foo) proxy).foo(), Is.is(foo));
    assertThat(((Bar) proxy).bar(), Is.is(bar));
  }

  interface Foo {
    int foo();
  }

  interface Bar {
    float bar();
  }

  static class Handler implements InvocationHandler, Serializable {

    private static final long serialVersionUID = 1L;

    static Method fooMethod, barMethod;

    static {
      try {
        fooMethod = Foo.class.getDeclaredMethod("foo");
        barMethod = Bar.class.getDeclaredMethod("bar");
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
    public Object invoke(Object proxy, Method method, Object[] args) {
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
