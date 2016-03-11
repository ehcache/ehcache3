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

package org.ehcache.core.internal.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

import org.ehcache.core.internal.util.ClassLoading;
import org.junit.Test;

public class ClassLoadingTest {

  @Test
  public void testDefaultClassLoader() throws Exception {
    String resource = getClass().getName().replace('.', '/').concat(".class");
    ClassLoader thisLoader = getClass().getClassLoader();
    ClassLoader defaultClassLoader = ClassLoading.getDefaultClassLoader();

    Thread.currentThread().setContextClassLoader(null);
    assertSame(thisLoader.loadClass(getClass().getName()), defaultClassLoader.loadClass(getClass().getName()));
    assertEquals(thisLoader.getResource(resource), defaultClassLoader.getResource(resource));
    assertEqualEnumeration(thisLoader.getResources(resource), defaultClassLoader.getResources(resource));

    Thread.currentThread().setContextClassLoader(new FindNothingLoader());
    assertSame(thisLoader.loadClass(getClass().getName()), defaultClassLoader.loadClass(getClass().getName()));
    assertEquals(thisLoader.getResource(resource), defaultClassLoader.getResource(resource));
    assertEqualEnumeration(thisLoader.getResources(resource), defaultClassLoader.getResources(resource));

    URL url = new URL("file:///tmp");
    ClassLoader tc = new TestClassLoader(url);
    Thread.currentThread().setContextClassLoader(tc);
    Class<?> c = defaultClassLoader.loadClass(getClass().getName());
    assertNotSame(getClass(), c);
    assertSame(tc, c.getClassLoader());
    assertEquals(url, defaultClassLoader.getResource(resource));
    assertEqualEnumeration(enumerationOf(url), defaultClassLoader.getResources(resource));
  }

  private static <T> Enumeration<T> enumerationOf(T... values) {
    Vector<T> v = new Vector<T>();
    for (T t : values) {
      v.add(t);
    }
    return v.elements();
  }

  private static class TestClassLoader extends ClassLoader {
    private final URL url;

    TestClassLoader(URL url) {
      super(null);
      this.url = url;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[1024];

      try {
        InputStream is = getClass().getClassLoader().getResourceAsStream(name.replace('.', '/').concat(".class"));
        int read;
        while ((read = is.read(buf)) >= 0) {
          baos.write(buf, 0, read);
        }
      } catch (IOException ioe) {
        throw new ClassNotFoundException();
      }

      byte[] data = baos.toByteArray();
      return defineClass(name, data, 0, data.length);
    }

    @Override
    public URL getResource(String name) {
      return url;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return enumerationOf(url);
    }

  }

  private static class FindNothingLoader extends ClassLoader {
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      throw new ClassNotFoundException();
    }

    @Override
    public URL getResource(String name) {
      return null;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return new Vector<URL>().elements();
    }
  }

  private void assertEqualEnumeration(Enumeration<?> e1, Enumeration<?> e2) {
    while (e1.hasMoreElements()) {
      if (!e2.hasMoreElements()) {
        throw new AssertionError();
      }

      Object o1 = e1.nextElement();
      Object o2 = e2.nextElement();

      if (o1 == null || o2 == null) {
        throw new AssertionError();
      }

      if ((!o1.equals(o2)) || (!o2.equals(o1))) {
        throw new AssertionError();
      }
    }

    if (e2.hasMoreElements()) {
      throw new AssertionError();
    }
  }

}
