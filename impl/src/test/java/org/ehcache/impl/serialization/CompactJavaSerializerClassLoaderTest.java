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
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import static org.ehcache.impl.serialization.SerializerTestUtilities.popTccl;
import static org.ehcache.impl.serialization.SerializerTestUtilities.pushTccl;

import org.ehcache.impl.serialization.CompactJavaSerializer;
import org.ehcache.spi.serialization.Serializer;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author teck
 */
public class CompactJavaSerializerClassLoaderTest {

  private static ClassLoader newLoader() {
    return new URLClassLoader(((URLClassLoader) CompactJavaSerializerClassLoaderTest.class.getClassLoader()).getURLs(), null);
  }

  @Test
  public void testThreadContextLoader() throws Exception {
    Serializer<Serializable> serializer = new CompactJavaSerializer(null);

    ClassLoader loader = newLoader();
    ByteBuffer encoded = serializer.serialize((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    pushTccl(loader);
    try {
      Assert.assertSame(loader, serializer.read(encoded).getClass().getClassLoader());
    } finally {
      popTccl();
    }
  }

  @Test
  public void testExplicitLoader() throws Exception {
    ClassLoader loader = newLoader();
    Serializer<Serializable> serializer = new CompactJavaSerializer(loader);

    ByteBuffer encoded = serializer.serialize((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    // setting TCCL doesn't matter here, but set it to make sure it doesn't get used
    pushTccl(newLoader());
    try {
      Assert.assertSame(loader, serializer.read(encoded).getClass().getClassLoader());
    } finally {
      popTccl();
    }
  }

  @SuppressWarnings("serial")
  public static class Foo implements Serializable {
    //
  }

}
