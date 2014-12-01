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

package org.ehcache.internal.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class JavaSerializerTest {

  @Test
  public void testBasic() throws Exception {
    MyClassLoader loader = new MyClassLoader();
    JavaSerializer<Serializable> js = new JavaSerializer<Serializable>(loader);

    ByteBuffer serialized = js.serialize(new MySerializableObject(42));
    MySerializableObject read = (MySerializableObject) js.read(serialized);
    assertEquals(42, read.getValue());
    
    assertTrue(loader.loaded.contains("org.ehcache.internal.serialization.JavaSerializerTest$MySerializableObject"));
  }

  @Test
  public void testProxy() throws Exception {
    MyClassLoader loader = new MyClassLoader();
    JavaSerializer<Serializable> js = new JavaSerializer<Serializable>(loader);

    ByteBuffer serialized = js.serialize((Serializable) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { Interface.class }, new Handler()));
    Interface proxy = (Interface) js.read(serialized);
    Handler handler = proxy.getHandler();
    assertEquals(handler, Proxy.getInvocationHandler(proxy));
    
    assertTrue(loader.loaded.contains("org.ehcache.internal.serialization.JavaSerializerTest$Interface"));
    assertTrue(loader.loaded.contains("org.ehcache.internal.serialization.JavaSerializerTest$Handler"));
  }

  private static class MyClassLoader extends ClassLoader {
    final Set<String> loaded = new HashSet<String>();
    
    MyClassLoader() {
      super(null);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      loaded.add(name);
      return getClass().getClassLoader().loadClass(name);
    }
  }

  @SuppressWarnings("serial")
  public static class Handler implements InvocationHandler, Serializable {

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return this;
    }

  }

  public interface Interface {
    Handler getHandler();
  }

  @SuppressWarnings("serial")
  private static class MySerializableObject implements Serializable {

    private final int value;

    MySerializableObject(int value) {
      this.value = value;
    }

    int getValue() {
      return value;
    }

  }

}
