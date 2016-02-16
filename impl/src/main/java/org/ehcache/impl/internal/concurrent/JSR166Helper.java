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
package org.ehcache.impl.internal.concurrent;

import java.lang.reflect.Field;

/**
 * @author Ludovic Orban
 */
public final class JSR166Helper {

    private JSR166Helper() {
    }

    public interface BiConsumer<A,B> { void accept(A a, B b); }
    public interface Consumer<A> { void accept(A a); }
    public interface ToLongFunction<A> { long applyAsLong(A a); }
    public interface ToLongBiFunction<A,B> { long applyAsLong(A a, B b); }
    public interface ToIntFunction<A> {int applyAsInt(A a); }
    public interface ToIntBiFunction<A,B> {int applyAsInt(A a, B b); }
    public interface ToDoubleFunction<A> { double applyAsDouble(A a); }
    public interface ToDoubleBiFunction<A,B> { double applyAsDouble(A a, B b); }
    public interface LongBinaryOperator { long applyAsLong(long a, long b); }
    public interface DoubleBinaryOperator { long applyAsDouble(double a, double b); }
    public interface IntBinaryOperator { int applyAsInt(int a, int b); }

    public static interface Spliterator<T> {
        static final int DISTINCT   = 0x00000001;
        static final int NONNULL    = 0x00000100;
        static final int CONCURRENT = 0x00001000;

        static final int SIZED      = 0x00000040;
        static final int IMMUTABLE  = 0x00000400;
        static final int SUBSIZED   = 0x00004000;

        Spliterator<T> trySplit();
        long estimateSize();
        void forEachRemaining(Consumer<? super T> action);
        boolean tryAdvance(Consumer<? super T> action);
        int characteristics();

        interface OfInt { }
        interface OfLong { }
        interface OfDouble { }
    }

    // only used by StreamSupport and ThreadLocalRandom
    interface IntStream {}
    interface IntConsumer { void accept(int i); }
    interface LongStream {}
    interface LongConsumer { void accept(long l); }
    interface DoubleStream {}
    interface DoubleConsumer { void accept(double v); }

    // only used by ThreadLocalRandom
    static class StreamSupport {
        static IntStream intStream(Spliterator.OfInt soi, boolean b) { throw new UnsupportedOperationException(); }
        static LongStream longStream(Spliterator.OfLong sol, boolean b) { throw new UnsupportedOperationException(); }
        static DoubleStream doubleStream(Spliterator.OfDouble sod, boolean b) { throw new UnsupportedOperationException(); }
    }

    // sun.misc.Unsafe wrapper that implements JDK8 intrinsics not available on JDK6
    static final class Unsafe {
        private static final sun.misc.Unsafe SMU;
        private static final Unsafe U;

        private static sun.misc.Unsafe getSMU() {
            try {
                return sun.misc.Unsafe.getUnsafe();
            } catch (SecurityException tryReflectionInstead) {
                // ignore
            }
            try {
                return java.security.AccessController.doPrivileged
                    (new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                        public sun.misc.Unsafe run() throws Exception {
                            Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                            for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                                f.setAccessible(true);
                                Object x = f.get(null);
                                if (k.isInstance(x))
                                    return k.cast(x);
                            }
                            throw new NoSuchFieldError("the Unsafe");
                        }
                    });
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics", e.getCause());
            }
        }

        static {
            try {
                SMU = getSMU();
                U = new Unsafe();
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        private Unsafe() {
        }

        public static Unsafe getUnsafe() {
            return U;
        }

        public long    objectFieldOffset(Field pending) { return SMU.objectFieldOffset(pending); }
        public int     arrayBaseOffset(Class<?> aClass) { return SMU.arrayBaseOffset(aClass); }
        public int     arrayIndexScale(Class<?> aClass) { return SMU.arrayIndexScale(aClass); }
        public boolean compareAndSwapInt(Object o, long pending, int c, int i) { return SMU.compareAndSwapInt(o, pending, c, i); }
        public boolean compareAndSwapLong(Object o, long l, long l1, long l2) { return SMU.compareAndSwapLong(o, l, l1, l2); }
        public boolean compareAndSwapObject(Object o, long l, Object o1, Object o2) { return SMU.compareAndSwapObject(o, l, o1, o2); }
        public void    park(boolean b, long l) { SMU.park(b, l); }
        public void    unpark(Object o) { SMU.unpark(o); }
        public void    putInt(Object o, long l, int i) { SMU.putInt(o, l, i); }
        public void    putOrderedInt(Object o, long qtop, int i) { SMU.putOrderedInt(o, qtop, i); }
        public void    putObject(Object o, long l, Object o1) { SMU.putObject(o, l, o1); }
        public void    putOrderedObject(Object o, long l, Object o1) { SMU.putOrderedObject(o, l, o1); }
        public Object  getObject(Object o, long l) { return SMU.getObject(o, l); }
        public Object  getObjectVolatile(Object o, long l) { return SMU.getObjectVolatile(o, l); }
        public void    putObjectVolatile(Object o, long l, Object o1) { SMU.putObjectVolatile(o, l, o1); }

        public int getAndAddInt(Object o, long offset, int val) {
            while (true) {
                int temp = SMU.getIntVolatile(o, offset);
                if (SMU.compareAndSwapInt(o, offset, temp, temp + val)) { return temp; }
            }
        }
        public long getAndAddLong(Object o, long offset, long val) {
            while (true) {
                long temp = SMU.getLongVolatile(o, offset);
                if (SMU.compareAndSwapLong(o, offset, temp, temp + val)) { return temp; }
            }
        }
        public Object getAndSetObject(Object o, long offset, Object val) {
            while (true) {
                Object temp = SMU.getObjectVolatile(o, offset);
                if (SMU.compareAndSwapObject(o, offset, temp, val)) { return temp; }
            }
        }
    }

}
