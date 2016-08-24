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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

/**
 * A classloader that attempts to load duplicate copies of classes loaded in another classloader.
 * <p>
 * This class is used in tests of class unloading behavior since the reachability of the newly loaded classes can be
 * more easily controlled.
 */
public class DuplicateClassLoader extends ClassLoader {

  private final ClassLoader original;

  public DuplicateClassLoader(ClassLoader original) {
    super(null);
    this.original = original;
  }

  @Override
  protected Enumeration<URL> findResources(String string) throws IOException {
    return original.getResources(string);
  }

  @Override
  protected URL findResource(String string) {
    return original.getResource(string);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    InputStream resource = getResourceAsStream(name.replace('.', '/') + ".class");
    if (resource == null) {
      throw new ClassNotFoundException(name);
    }
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      try {
        while (true) {
          int read = resource.read();
          if (read == -1) {
            return defineClass(name, bout.toByteArray(), 0, bout.size());
          } else {
            bout.write(read);
          }
        }
      } finally {
        bout.close();
      }
    } catch (IOException ex) {
      throw new ClassNotFoundException(name, ex);
    } finally {
      try {
        resource.close();
      } catch (IOException ex) {
        //ignore
      }
    }
  }



}
