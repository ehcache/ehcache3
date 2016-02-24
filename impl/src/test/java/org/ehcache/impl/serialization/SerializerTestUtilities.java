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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;

/**
 *
 * @author cdennis
 */
public final class SerializerTestUtilities {

  private SerializerTestUtilities() {
    //no instances please
  }

  public static ClassLoader createClassNameRewritingLoader(Class<?> initial, Class<?> ... more) {
    ClassLoader loader = initial.getClassLoader();
    Map<String, String> remapping = new HashMap<String, String>();
    remapping.putAll(createRemappings(initial));
    for (Class<?> klazz : more) {
      remapping.putAll(createRemappings(klazz));
    }
    return new RewritingClassloader(loader, remapping);
  }

  private static Map<String, String> createRemappings(Class<?> initial) {
    HashMap<String, String> remappings = new HashMap<String, String>();
    remappings.put(initial.getName(), newClassName(initial));
    for (Class<?> inner : initial.getDeclaredClasses()) {
      remappings.put(inner.getName(), newClassName(inner));
    }
    if (initial.isEnum()) {
      for (Object e : initial.getEnumConstants()) {
        Class<?> eClass = e.getClass();
        if (eClass != initial) {
          remappings.put(eClass.getName(), newClassName(eClass));
        }
      }
    }
    return remappings;
  }

  public static String newClassName(Class<?> initial) {
    String initialName = initial.getName();
    int lastUnderscore = initialName.lastIndexOf('_');
    if (lastUnderscore == -1) {
      return initialName;
    } else {
      int nextDollar = initialName.indexOf('$', lastUnderscore);
      if (nextDollar == -1) {
        return initialName.substring(0, lastUnderscore);
      } else {
        return initialName.substring(0, lastUnderscore).concat(initialName.substring(nextDollar));
      }
    }
  }

  private static final ThreadLocal<Deque<ClassLoader>> tcclStacks = new ThreadLocal<Deque<ClassLoader>>() {
    @Override
    protected Deque<ClassLoader> initialValue() {
      return new LinkedList<ClassLoader>();
    }
  };

  public static void pushTccl(ClassLoader loader) {
    tcclStacks.get().push(Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(loader);
  }

  public static void popTccl() {
    Thread.currentThread().setContextClassLoader(tcclStacks.get().pop());
  }

  static class RewritingClassloader extends ClassLoader {

    private final Map<String, String> remappings;

    RewritingClassloader(ClassLoader parent, Map<String, String> remappings) {
      super(parent);
      this.remappings = Collections.unmodifiableMap(new HashMap<String, String>(remappings));
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      Class<?> c = findLoadedClass(name);
      if (c == null) {
        if (remappings.containsValue(name)) {
          c = findClass(name);
          if (resolve) {
            resolveClass(c);
          }
        } else {
          return super.loadClass(name, resolve);
        }
      }
      return c;
    }


    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      for (Entry<String, String> mapping : remappings.entrySet()) {
        if (name.equals(mapping.getValue())) {
          String path = mapping.getKey().replace('.', '/').concat(".class");
          try {
            InputStream resource = getResourceAsStream(path);
            try {
              ClassReader reader = new ClassReader(resource);

              ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
              ClassVisitor visitor = new RemappingClassAdapter(writer, new Remapper() {

                @Override
                public String map(String from) {
                  String to = remappings.get(from.replace('/', '.'));
                  if (to == null) {
                    return from;
                  } else {
                    return to.replace('.', '/');
                  }
                }
              });

              reader.accept(visitor, ClassReader.EXPAND_FRAMES);
              byte[] classBytes = writer.toByteArray();

              return defineClass(name, classBytes, 0, classBytes.length);
            } finally {
              resource.close();
            }
          } catch (IOException e) {
            throw new ClassNotFoundException("IOException while loading", e);
          }
        }
      }
      return super.findClass(name);
    }
  }
}
