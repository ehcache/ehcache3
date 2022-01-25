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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.spi.serialization.Serializer;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.InstructionAdapter;
import org.objectweb.asm.commons.Method;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.objectweb.asm.Opcodes.ASM6;
import static org.objectweb.asm.Type.getObjectType;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

public class AssertingOffHeapValueHolder<V> extends LazyOffHeapValueHolder<V> {

  /**
   * This is a set of 'patterns' that capture a subset of the lock scopes that the `OffHeapValueHolder` can be
   * called from.  You might end up looking at these patterns for one of three reasons:
   *
   * 1. (Most Likely) You introduced new code or new testing that access a pre-existing lock scope that is not listed
   *    here.  Find the lock scope in the stack trace of the thrown exception and add an appropriate stanza here.
   * 2. (Less Likely) You introduced a new call path that leaks a still serialized, attached value holder that someone
   *    else tried to access later (outside lock scope).  In this case you must locate the source of the value holder
   *    and either force deserialization or detach the value under the pre-existing lock scope.
   * 3. (Least Likely) You introduced a new method to OffHeapStore that needs locking properly.
   */
  private static final Collection<Predicate<StackTraceElement>> LOCK_SCOPES = asList(
    className("org.terracotta.offheapstore.AbstractLockedOffHeapHashMap").methodName("shrink"),
    className("org.terracotta.offheapstore.AbstractLockedOffHeapHashMap").methodName("computeWithMetadata"),
    className("org.terracotta.offheapstore.AbstractLockedOffHeapHashMap").methodName("computeIfPresentWithMetadata"),
    className("org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory$EhcacheSegment$EntrySet").methodName("iterator"),
    className("org.ehcache.impl.internal.store.disk.factories.EhcachePersistentSegmentFactory$EhcachePersistentSegment$EntrySet").methodName("iterator"),
    className("org.terracotta.offheapstore.AbstractLockedOffHeapHashMap$LockedEntryIterator").methodName("next")
  );

  private static void assertStackTraceContainsLockScope() {
    assertThat(stream(Thread.currentThread().getStackTrace()).filter(ste -> LOCK_SCOPES.stream().anyMatch(p -> p.test(ste))).anyMatch(ste -> isLockedInFrame(ste)), is(true));
  }

  private static StePredicateBuilderOne className(String className) {
    return new StePredicateBuilderOne() {
      @Override
      public Predicate<StackTraceElement> methodName(String methodName) {
        StePredicateBuilderOne outer = this;
        return ste -> outer.test(ste) && methodName.equals(ste.getMethodName());
      }

      @Override
      public boolean test(StackTraceElement ste) {
        return className.equals(ste.getClassName());
      }
    };
  }

  interface StePredicateBuilderOne extends Predicate<StackTraceElement> {

    Predicate<StackTraceElement> methodName(String computeIfPresentWithMetadata);
  }

  public AssertingOffHeapValueHolder(long id, ByteBuffer binaryValue, Serializer<V> serializer, long creationTime, long expireTime, long lastAccessTime, WriteContext writeContext) {
    super(id, binaryValue, serializer, creationTime, expireTime, lastAccessTime, writeContext);
  }

  @Override
  void writeBack() {
    assertStackTraceContainsLockScope();
    super.writeBack();
  }

  @Override
  V deserialize() {
    if (!isBinaryValueAvailable()) {
      assertStackTraceContainsLockScope();
    }
    return super.deserialize();
  }

  @Override
  void detach() {
    assertStackTraceContainsLockScope();
    super.detach();
  }

  private static final Type LOCK_CLASS;
  private static final Method LOCK_METHOD;
  private static final Method UNLOCK_METHOD;

  static {
    try {
      LOCK_CLASS = getType(Lock.class);
      LOCK_METHOD = getMethod(Lock.class.getMethod("lock"));
      UNLOCK_METHOD = getMethod(Lock.class.getMethod("unlock"));
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  private static boolean isLockedInFrame(StackTraceElement ste) {
    try {
      ClassReader reader = new ClassReader(ste.getClassName());

      NavigableMap<Integer, Integer> lockLevels = new TreeMap<>();

      reader.accept(new ClassVisitor(ASM6) {
        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
          if (ste.getMethodName().equals(name)) {
            return new InstructionAdapter(ASM6, new MethodVisitor(ASM6) {}) {

              private final Map<Label, Integer> levels = new HashMap<>();

              private int lockLevel;

              @Override
              public void invokeinterface(String owner, String name, String descriptor) {
                if (LOCK_CLASS.equals(getObjectType(owner))) {
                  if (LOCK_METHOD.equals(new Method(name, descriptor))) {
                    lockLevel++;
                  } else if (UNLOCK_METHOD.equals(new Method(name, descriptor))) {
                    lockLevel--;
                  }
                }
              }

              @Override
              public void visitJumpInsn(int opcode, Label label) {
                levels.merge(label, lockLevel, Integer::max);
              }

              @Override
              public void visitLabel(Label label) {
                lockLevel = levels.merge(label, lockLevel, Integer::max);
              }

              @Override
              public void visitLineNumber(int line, Label start) {
                lockLevels.merge(line, levels.get(start), Integer::max);
              }
            };
          } else {
            return null;
          }
        }
      }, 0);

      Map.Entry<Integer, Integer> entry = lockLevels.floorEntry(ste.getLineNumber());

      return entry.getValue() > 0;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}
