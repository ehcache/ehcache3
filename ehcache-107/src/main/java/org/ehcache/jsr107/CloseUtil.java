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

package org.ehcache.jsr107;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public class CloseUtil {
  public static <T extends Throwable> T closeAllAfter(T failure, Object ... objects) {
    Optional<Closeable> closeable = extractCloseables(Stream.of(objects)).reduce(CloseUtil::composeCloseables);
    if (closeable.isPresent()) {
      try {
        closeable.get().close();
      } catch (Throwable t) {
        failure.addSuppressed(t);
      }
    }
    return failure;
  }

  static void closeAll(Object ... objects) throws IOException {
    closeAll(Stream.of(objects));
  }

  static void closeAll(Stream<Object> objects) throws IOException {
    chain(extractCloseables(objects));
  }

  static void chain(Closeable ... objects) throws IOException {
    chain(Stream.of(objects));
  }

  public static void chain(Stream<Closeable> objects) throws IOException {
    Optional<Closeable> closeable = objects.reduce(CloseUtil::composeCloseables);
    if (closeable.isPresent()) {
      closeable.get().close();
    }
  }

  private static Stream<Closeable> extractCloseables(Stream<Object> objects) {
    return objects.filter(o -> o != null).flatMap(o -> {
      if (o instanceof Collection<?>) {
        return ((Collection<?>) o).stream();
      } else if (o.getClass().isArray()) {
        return Arrays.stream((Object[]) o);
      } else {
        return Stream.of(o);
      }
    }).filter(o -> o != null).filter(Closeable.class::isInstance).map(Closeable.class::cast);
  }

  private static Closeable composeCloseables(Closeable a, Closeable b) {
    return () -> {
      try {
        a.close();
      } catch (Throwable t1) {
        try {
          b.close();
        } catch (Throwable t2) {
          t1.addSuppressed(t2);
        }
        throw t1;
      }
      b.close();
    };
  }

}
