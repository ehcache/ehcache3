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

import org.ehcache.clustered.common.internal.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

public class Util {

  public static Object unmarshall(ByteBuffer payload, Predicate<Class<?>> isClassPermitted) {
    try (ObjectInputStream objectInputStream =
           new FilteredObjectInputStream(new ByteBufferInputStream(payload), isClassPermitted, null)) {
      return objectInputStream.readObject();
    } catch (IOException | ClassNotFoundException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static byte[] marshall(Object message) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try(ObjectOutputStream oout = new ObjectOutputStream(out)) {
      oout.writeObject(message);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return out.toByteArray();
  }
}
