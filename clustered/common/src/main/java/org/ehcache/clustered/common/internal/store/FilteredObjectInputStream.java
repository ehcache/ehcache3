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
import java.io.InvalidClassException;
import java.io.ObjectStreamClass;
import java.util.function.Predicate;

public class FilteredObjectInputStream extends CustomLoaderBasedObjectInputStream {

  private Predicate<Class<?>> isClassPermitted;

  public FilteredObjectInputStream(InputStream in, Predicate<Class<?>> isClassPermitted, ClassLoader loader) throws IOException {
    super(in, loader);
    this.isClassPermitted = isClassPermitted;
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass serialInput) throws IOException, ClassNotFoundException {
    final Class<?> c = super.resolveClass(serialInput);

    if (!isClassPermitted.test(c)) {
      throw new InvalidClassException("Class deserialization of " + c.getName() + " blocked.");
    }

    return c;
  }
}
