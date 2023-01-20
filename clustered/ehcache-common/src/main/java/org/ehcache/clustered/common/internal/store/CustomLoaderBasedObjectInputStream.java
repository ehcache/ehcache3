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

package org.ehcache.clustered.common.internal.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class CustomLoaderBasedObjectInputStream extends ObjectInputStream {

  private final ClassLoader loader;

  public CustomLoaderBasedObjectInputStream(InputStream in, ClassLoader loader) throws IOException {
    super(in);
    this.loader = loader;
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    try {
      final ClassLoader cl = loader == null ? Thread.currentThread().getContextClassLoader() : loader;
      if (cl == null) {
        return super.resolveClass(desc);
      } else {
        try {
          return Class.forName(desc.getName(), false, cl);
        } catch (ClassNotFoundException e) {
          return super.resolveClass(desc);
        }
      }
    } catch (SecurityException ex) {
      return super.resolveClass(desc);
    }
  }
}
