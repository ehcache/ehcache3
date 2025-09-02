/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This is a modified version of the original Apache class.  It has had unused
 * members removed.
 */
package org.ehcache.impl.internal.classes.commonslang;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Operates on classes without using reflection.
 *
 * <p>
 * This class handles invalid {@code null} inputs as best it can. Each method documents its behavior in more detail.
 * </p>
 *
 * <p>
 * The notion of a {@code canonical name} includes the human-readable name for the type, for example {@code int[]}. The
 * non-canonical method variants work with the JVM names, such as {@code [I}.
 * </p>
 *
 * @since 2.0
 */
public class ClassUtils {

    /**
     * Inclusivity literals for {@link #hierarchy(Class, Interfaces)}.
     *
     * @since 3.2
     */
    public enum Interfaces {

        /** Includes interfaces. */
        INCLUDE,

        /** Excludes interfaces. */
        EXCLUDE
    }

    /**
     * The package separator character: {@code '&#x2e;' == {@value}}.
     */
    public static final char PACKAGE_SEPARATOR_CHAR = '.';

    /**
     * The package separator String: {@code "&#x2e;"}.
     */
    public static final String PACKAGE_SEPARATOR = String.valueOf(PACKAGE_SEPARATOR_CHAR);

    /**
     * The inner class separator character: {@code '$' == {@value}}.
     */
    public static final char INNER_CLASS_SEPARATOR_CHAR = '$';

    /**
     * The inner class separator String: {@code "$"}.
     */
    public static final String INNER_CLASS_SEPARATOR = String.valueOf(INNER_CLASS_SEPARATOR_CHAR);

    /**
     * Maps names of primitives to their corresponding primitive {@link Class}es.
     */
    private static final Map<String, Class<?>> NAME_PRIMITIVE_MAP = new HashMap<>();

    static {
        NAME_PRIMITIVE_MAP.put(Boolean.TYPE.getName(), Boolean.TYPE);
        NAME_PRIMITIVE_MAP.put(Byte.TYPE.getName(), Byte.TYPE);
        NAME_PRIMITIVE_MAP.put(Character.TYPE.getName(), Character.TYPE);
        NAME_PRIMITIVE_MAP.put(Double.TYPE.getName(), Double.TYPE);
        NAME_PRIMITIVE_MAP.put(Float.TYPE.getName(), Float.TYPE);
        NAME_PRIMITIVE_MAP.put(Integer.TYPE.getName(), Integer.TYPE);
        NAME_PRIMITIVE_MAP.put(Long.TYPE.getName(), Long.TYPE);
        NAME_PRIMITIVE_MAP.put(Short.TYPE.getName(), Short.TYPE);
        NAME_PRIMITIVE_MAP.put(Void.TYPE.getName(), Void.TYPE);
    }

    /**
     * Maps primitive {@link Class}es to their corresponding wrapper {@link Class}.
     */
    private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP = new HashMap<>();

    static {
        PRIMITIVE_WRAPPER_MAP.put(Boolean.TYPE, Boolean.class);
        PRIMITIVE_WRAPPER_MAP.put(Byte.TYPE, Byte.class);
        PRIMITIVE_WRAPPER_MAP.put(Character.TYPE, Character.class);
        PRIMITIVE_WRAPPER_MAP.put(Short.TYPE, Short.class);
        PRIMITIVE_WRAPPER_MAP.put(Integer.TYPE, Integer.class);
        PRIMITIVE_WRAPPER_MAP.put(Long.TYPE, Long.class);
        PRIMITIVE_WRAPPER_MAP.put(Double.TYPE, Double.class);
        PRIMITIVE_WRAPPER_MAP.put(Float.TYPE, Float.class);
        PRIMITIVE_WRAPPER_MAP.put(Void.TYPE, Void.TYPE);
    }

    /**
     * Maps wrapper {@link Class}es to their corresponding primitive types.
     */
    private static final Map<Class<?>, Class<?>> WRAPPER_PRIMITIVE_MAP = new HashMap<>();

    static {
        PRIMITIVE_WRAPPER_MAP.forEach((primitiveClass, wrapperClass) -> {
            if (!primitiveClass.equals(wrapperClass)) {
                WRAPPER_PRIMITIVE_MAP.put(wrapperClass, primitiveClass);
            }
        });
    }

    /**
     * Given a {@link List} of {@link Class} objects, this method converts them into class names.
     *
     * <p>
     * A new {@link List} is returned. {@code null} objects will be copied into the returned list as {@code null}.
     * </p>
     *
     * @param classes the classes to change
     * @return a {@link List} of class names corresponding to the Class objects, {@code null} if null input
     * @throws ClassCastException if {@code classes} contains a non-{@link Class} entry
     */
    public static List<String> convertClassesToClassNames(final List<Class<?>> classes) {
        return classes == null ? null : classes.stream().map(e -> getName(e, null)).collect(Collectors.toList());
    }

    /**
     * Given a {@link List} of class names, this method converts them into classes.
     *
     * <p>
     * A new {@link List} is returned. If the class name cannot be found, {@code null} is stored in the {@link List}. If the
     * class name in the {@link List} is {@code null}, {@code null} is stored in the output {@link List}.
     * </p>
     *
     * @param classNames the classNames to change
     * @return a {@link List} of Class objects corresponding to the class names, {@code null} if null input
     * @throws ClassCastException if classNames contains a non String entry
     */
    public static List<Class<?>> convertClassNamesToClasses(final List<String> classNames) {
        if (classNames == null) {
            return null;
        }
        final List<Class<?>> classes = new ArrayList<>(classNames.size());
        classNames.forEach(className -> {
            try {
                classes.add(Class.forName(className));
            } catch (final Exception ex) {
                classes.add(null);
            }
        });
        return classes;
    }

    /**
     * Gets a {@link List} of all interfaces implemented by the given class and its superclasses.
     *
     * <p>
     * The order is determined by looking through each interface in turn as declared in the source file and following its
     * hierarchy up. Then each superclass is considered in the same way. Later duplicates are ignored, so the order is
     * maintained.
     * </p>
     *
     * @param cls the class to look up, may be {@code null}
     * @return the {@link List} of interfaces in order, {@code null} if null input
     */
    public static List<Class<?>> getAllInterfaces(final Class<?> cls) {
        if (cls == null) {
            return null;
        }

        final LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
        getAllInterfaces(cls, interfacesFound);

        return new ArrayList<>(interfacesFound);
    }

    /**
     * Gets the interfaces for the specified class.
     *
     * @param cls the class to look up, may be {@code null}
     * @param interfacesFound the {@link Set} of interfaces for the class
     */
    private static void getAllInterfaces(Class<?> cls, final HashSet<Class<?>> interfacesFound) {
        while (cls != null) {
            final Class<?>[] interfaces = cls.getInterfaces();

            for (final Class<?> i : interfaces) {
                if (interfacesFound.add(i)) {
                    getAllInterfaces(i, interfacesFound);
                }
            }

            cls = cls.getSuperclass();
        }
    }

    /**
     * Gets a {@link List} of superclasses for the given class.
     *
     * @param cls the class to look up, may be {@code null}
     * @return the {@link List} of superclasses in order going up from this one {@code null} if null input
     */
    public static List<Class<?>> getAllSuperclasses(final Class<?> cls) {
        if (cls == null) {
            return null;
        }
        final List<Class<?>> classes = new ArrayList<>();
        Class<?> superclass = cls.getSuperclass();
        while (superclass != null) {
            classes.add(superclass);
            superclass = superclass.getSuperclass();
        }
        return classes;
    }

    /**
     * Gets the canonical name for a {@link Class}.
     *
     * @param cls the class for which to get the canonical class name; may be null
     * @param valueIfNull the return value if null
     * @return the canonical name of the class, or {@code valueIfNull}
     * @since 3.7
     * @see Class#getCanonicalName()
     */
    public static String getCanonicalName(final Class<?> cls, final String valueIfNull) {
        if (cls == null) {
            return valueIfNull;
        }
        final String canonicalName = cls.getCanonicalName();
        return canonicalName == null ? valueIfNull : canonicalName;
    }

    /**
     * Gets the canonical name for an {@link Object}.
     *
     * @param object the object for which to get the canonical class name; may be null
     * @param valueIfNull the return value if null
     * @return the canonical name of the object or {@code valueIfNull}
     * @since 3.7
     * @see Class#getCanonicalName()
     */
    public static String getCanonicalName(final Object object, final String valueIfNull) {
        if (object == null) {
            return valueIfNull;
        }
        final String canonicalName = object.getClass().getCanonicalName();
        return canonicalName == null ? valueIfNull : canonicalName;
    }

    /**
     * Delegates to {@link Class#getComponentType()} using generics.
     *
     * @param <T> The array class type.
     * @param cls A class or null.
     * @return The array component type or null.
     * @see Class#getComponentType()
     * @since 3.13.0
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getComponentType(final Class<T[]> cls) {
        return cls == null ? null : (Class<T>) cls.getComponentType();
    }

    /**
     * Null-safe version of {@code cls.getName()}
     *
     * @param cls the class for which to get the class name; may be null
     * @param valueIfNull the return value if the argument {@code cls} is {@code null}
     * @return the class name or {@code valueIfNull}
     * @since 3.7
     * @see Class#getName()
     */
    public static String getName(final Class<?> cls, final String valueIfNull) {
        return cls == null ? valueIfNull : cls.getName();
    }

    /**
     * Null-safe version of {@code object.getClass().getSimpleName()}
     *
     * @param object the object for which to get the class name; may be null
     * @param valueIfNull the value to return if {@code object} is {@code null}
     * @return the class name or {@code valueIfNull}
     * @since 3.0
     * @see Class#getName()
     */
    public static String getName(final Object object, final String valueIfNull) {
        return object == null ? valueIfNull : object.getClass().getName();
    }

    /**
     * Gets the primitive class for the given class name, for example "byte".
     *
     * @param className the primitive class for the given class name.
     * @return the primitive class.
     */
    static Class<?> getPrimitiveClass(final String className) {
        return NAME_PRIMITIVE_MAP.get(className);
    }

    /**
     * Null-safe version of {@code cls.getSimpleName()}
     *
     * @param cls the class for which to get the simple name; may be null
     * @param valueIfNull the value to return if null
     * @return the simple class name or {@code valueIfNull} if the argument {@code cls} is {@code null}
     * @since 3.0
     * @see Class#getSimpleName()
     */
    public static String getSimpleName(final Class<?> cls, final String valueIfNull) {
        return cls == null ? valueIfNull : cls.getSimpleName();
    }

    /**
     * Null-safe version of {@code object.getClass().getSimpleName()}
     *
     * @param object the object for which to get the simple class name; may be null
     * @param valueIfNull the value to return if {@code object} is {@code null}
     * @return the simple class name or {@code valueIfNull} if the argument {@code object} is {@code null}
     * @since 3.0
     * @see Class#getSimpleName()
     */
    public static String getSimpleName(final Object object, final String valueIfNull) {
        return object == null ? valueIfNull : object.getClass().getSimpleName();
    }

    /**
     * Gets an {@link Iterable} that can iterate over a class hierarchy in ascending (subclass to superclass) order,
     * excluding interfaces.
     *
     * @param type the type to get the class hierarchy from
     * @return Iterable an Iterable over the class hierarchy of the given class
     * @since 3.2
     */
    public static Iterable<Class<?>> hierarchy(final Class<?> type) {
        return hierarchy(type, Interfaces.EXCLUDE);
    }

    /**
     * Gets an {@link Iterable} that can iterate over a class hierarchy in ascending (subclass to superclass) order.
     *
     * @param type the type to get the class hierarchy from
     * @param interfacesBehavior switch indicating whether to include or exclude interfaces
     * @return Iterable an Iterable over the class hierarchy of the given class
     * @since 3.2
     */
    public static Iterable<Class<?>> hierarchy(final Class<?> type, final Interfaces interfacesBehavior) {
        final Iterable<Class<?>> classes = () -> {
            final AtomicReference<Class<?>> next = new AtomicReference<>(type);
            return new Iterator<Class<?>>() {

                @Override
                public boolean hasNext() {
                    return next.get() != null;
                }

                @Override
                public Class<?> next() {
                    return next.getAndUpdate(Class::getSuperclass);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        };
        if (interfacesBehavior != Interfaces.INCLUDE) {
            return classes;
        }
        return () -> {
            final Set<Class<?>> seenInterfaces = new HashSet<>();
            final Iterator<Class<?>> wrapped = classes.iterator();

            return new Iterator<Class<?>>() {
                Iterator<Class<?>> interfaces = Collections.emptyIterator();

                @Override
                public boolean hasNext() {
                    return interfaces.hasNext() || wrapped.hasNext();
                }

                @Override
                public Class<?> next() {
                    if (interfaces.hasNext()) {
                        final Class<?> nextInterface = interfaces.next();
                        seenInterfaces.add(nextInterface);
                        return nextInterface;
                    }
                    final Class<?> nextSuperclass = wrapped.next();
                    final Set<Class<?>> currentInterfaces = new LinkedHashSet<>();
                    walkInterfaces(currentInterfaces, nextSuperclass);
                    interfaces = currentInterfaces.iterator();
                    return nextSuperclass;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                private void walkInterfaces(final Set<Class<?>> addTo, final Class<?> c) {
                    for (final Class<?> iface : c.getInterfaces()) {
                        if (!seenInterfaces.contains(iface)) {
                            addTo.add(iface);
                        }
                        walkInterfaces(addTo, iface);
                    }
                }

            };
        };
    }

    /**
     * Checks if one {@link Class} can be assigned to a variable of another {@link Class}.
     *
     * <p>
     * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of
     * primitive classes and {@code null}s.
     * </p>
     *
     * <p>
     * Primitive widenings allow an int to be assigned to a long, float or double. This method returns the correct result
     * for these cases.
     * </p>
     *
     * <p>
     * {@code null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in
     * and the toClass is non-primitive.
     * </p>
     *
     * <p>
     * Specifically, this method tests whether the type represented by the specified {@link Class} parameter can be
     * converted to the type represented by this {@link Class} object via an identity conversion widening primitive or
     * widening reference conversion. See <em><a href="https://docs.oracle.com/javase/specs/">The Java Language
     * Specification</a></em>, sections 5.1.1, 5.1.2 and 5.1.4 for details.
     * </p>
     *
     * <p>
     * <strong>Since Lang 3.0,</strong> this method will default behavior for calculating assignability between primitive
     * and wrapper types <em>corresponding to the running Java version</em>; i.e. autoboxing will be the default behavior in
     * VMs running Java versions &gt; 1.5.
     * </p>
     *
     * @param cls the Class to check, may be null
     * @param toClass the Class to try to assign into, returns false if null
     * @return {@code true} if assignment possible
     */
    public static boolean isAssignable(final Class<?> cls, final Class<?> toClass) {
        return isAssignable(cls, toClass, true);
    }

    /**
     * Checks if one {@link Class} can be assigned to a variable of another {@link Class}.
     *
     * <p>
     * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of
     * primitive classes and {@code null}s.
     * </p>
     *
     * <p>
     * Primitive widenings allow an int to be assigned to a long, float or double. This method returns the correct result
     * for these cases.
     * </p>
     *
     * <p>
     * {@code null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in
     * and the toClass is non-primitive.
     * </p>
     *
     * <p>
     * Specifically, this method tests whether the type represented by the specified {@link Class} parameter can be
     * converted to the type represented by this {@link Class} object via an identity conversion widening primitive or
     * widening reference conversion. See <em><a href="https://docs.oracle.com/javase/specs/">The Java Language
     * Specification</a></em>, sections 5.1.1, 5.1.2 and 5.1.4 for details.
     * </p>
     *
     * @param cls the Class to check, may be null
     * @param toClass the Class to try to assign into, returns false if null
     * @param autoboxing whether to use implicit autoboxing/unboxing between primitives and wrappers
     * @return {@code true} if assignment possible
     */
    public static boolean isAssignable(Class<?> cls, final Class<?> toClass, final boolean autoboxing) {
        if (toClass == null) {
            return false;
        }
        // have to check for null, as isAssignableFrom doesn't
        if (cls == null) {
            return !toClass.isPrimitive();
        }
        // autoboxing:
        if (autoboxing) {
            if (cls.isPrimitive() && !toClass.isPrimitive()) {
                cls = primitiveToWrapper(cls);
                if (cls == null) {
                    return false;
                }
            }
            if (toClass.isPrimitive() && !cls.isPrimitive()) {
                cls = wrapperToPrimitive(cls);
                if (cls == null) {
                    return false;
                }
            }
        }
        if (cls.equals(toClass)) {
            return true;
        }
        if (cls.isPrimitive()) {
            if (!toClass.isPrimitive()) {
                return false;
            }
            if (Integer.TYPE.equals(cls)) {
                return Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
            }
            if (Long.TYPE.equals(cls)) {
                return Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
            }
            if (Boolean.TYPE.equals(cls)) {
                return false;
            }
            if (Double.TYPE.equals(cls)) {
                return false;
            }
            if (Float.TYPE.equals(cls)) {
                return Double.TYPE.equals(toClass);
            }
            if (Character.TYPE.equals(cls)  || Short.TYPE.equals(cls)) {
                return Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
            }
            if (Byte.TYPE.equals(cls)) {
                return Short.TYPE.equals(toClass) || Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass)
                    || Double.TYPE.equals(toClass);
            }
            // should never get here
            return false;
        }
        return toClass.isAssignableFrom(cls);
    }

    /**
     * Checks if an array of Classes can be assigned to another array of Classes.
     *
     * <p>
     * This method calls {@link #isAssignable(Class, Class) isAssignable} for each Class pair in the input arrays. It can be
     * used to check if a set of arguments (the first parameter) are suitably compatible with a set of method parameter
     * types (the second parameter).
     * </p>
     *
     * <p>
     * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of
     * primitive classes and {@code null}s.
     * </p>
     *
     * <p>
     * Primitive widenings allow an int to be assigned to a {@code long}, {@code float} or {@code double}. This method
     * returns the correct result for these cases.
     * </p>
     *
     * <p>
     * {@code null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in
     * and the toClass is non-primitive.
     * </p>
     *
     * <p>
     * Specifically, this method tests whether the type represented by the specified {@link Class} parameter can be
     * converted to the type represented by this {@link Class} object via an identity conversion widening primitive or
     * widening reference conversion. See <em><a href="https://docs.oracle.com/javase/specs/">The Java Language
     * Specification</a></em>, sections 5.1.1, 5.1.2 and 5.1.4 for details.
     * </p>
     *
     * <p>
     * <strong>Since Lang 3.0,</strong> this method will default behavior for calculating assignability between primitive
     * and wrapper types <em>corresponding to the running Java version</em>; i.e. autoboxing will be the default behavior in
     * VMs running Java versions &gt; 1.5.
     * </p>
     *
     * @param classArray the array of Classes to check, may be {@code null}
     * @param toClassArray the array of Classes to try to assign into, may be {@code null}
     * @return {@code true} if assignment possible
     */
    public static boolean isAssignable(final Class<?>[] classArray, final Class<?>... toClassArray) {
        return isAssignable(classArray, toClassArray, true);
    }

    /**
     * Checks if an array of Classes can be assigned to another array of Classes.
     *
     * <p>
     * This method calls {@link #isAssignable(Class, Class) isAssignable} for each Class pair in the input arrays. It can be
     * used to check if a set of arguments (the first parameter) are suitably compatible with a set of method parameter
     * types (the second parameter).
     * </p>
     *
     * <p>
     * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of
     * primitive classes and {@code null}s.
     * </p>
     *
     * <p>
     * Primitive widenings allow an int to be assigned to a {@code long}, {@code float} or {@code double}. This method
     * returns the correct result for these cases.
     * </p>
     *
     * <p>
     * {@code null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in
     * and the toClass is non-primitive.
     * </p>
     *
     * <p>
     * Specifically, this method tests whether the type represented by the specified {@link Class} parameter can be
     * converted to the type represented by this {@link Class} object via an identity conversion widening primitive or
     * widening reference conversion. See <em><a href="https://docs.oracle.com/javase/specs/">The Java Language
     * Specification</a></em>, sections 5.1.1, 5.1.2 and 5.1.4 for details.
     * </p>
     *
     * @param classArray the array of Classes to check, may be {@code null}
     * @param toClassArray the array of Classes to try to assign into, may be {@code null}
     * @param autoboxing whether to use implicit autoboxing/unboxing between primitives and wrappers
     * @return {@code true} if assignment possible
     */
    public static boolean isAssignable(Class<?>[] classArray, Class<?>[] toClassArray, final boolean autoboxing) {
        if (!ArrayUtils.isSameLength(classArray, toClassArray)) {
            return false;
        }
        classArray = ArrayUtils.nullToEmpty(classArray);
        toClassArray = ArrayUtils.nullToEmpty(toClassArray);
        for (int i = 0; i < classArray.length; i++) {
            if (!isAssignable(classArray[i], toClassArray[i], autoboxing)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is the specified class an inner class or static nested class.
     *
     * @param cls the class to check, may be null
     * @return {@code true} if the class is an inner or static nested class, false if not or {@code null}
     */
    public static boolean isInnerClass(final Class<?> cls) {
        return cls != null && cls.getEnclosingClass() != null;
    }

    /**
     * Returns whether the given {@code type} is a primitive or primitive wrapper ({@link Boolean}, {@link Byte},
     * {@link Character}, {@link Short}, {@link Integer}, {@link Long}, {@link Double}, {@link Float}).
     *
     * @param type The class to query or null.
     * @return true if the given {@code type} is a primitive or primitive wrapper ({@link Boolean}, {@link Byte},
     *         {@link Character}, {@link Short}, {@link Integer}, {@link Long}, {@link Double}, {@link Float}).
     * @since 3.1
     */
    public static boolean isPrimitiveOrWrapper(final Class<?> type) {
        if (type == null) {
            return false;
        }
        return type.isPrimitive() || isPrimitiveWrapper(type);
    }
    /**
     * Tests whether the given {@code type} is a primitive wrapper ({@link Boolean}, {@link Byte}, {@link Character},
     * {@link Short}, {@link Integer}, {@link Long}, {@link Double}, {@link Float}).
     *
     * @param type The class to query or null.
     * @return true if the given {@code type} is a primitive wrapper ({@link Boolean}, {@link Byte}, {@link Character},
     *         {@link Short}, {@link Integer}, {@link Long}, {@link Double}, {@link Float}).
     * @since 3.1
     */
    public static boolean isPrimitiveWrapper(final Class<?> type) {
        return WRAPPER_PRIMITIVE_MAP.containsKey(type);
    }

    /**
     * Tests whether a {@link Class} is public.
     * @param cls Class to test.
     * @return {@code true} if {@code cls} is public.
     * @since 3.13.0
     */
    public static boolean isPublic(final Class<?> cls) {
        return Modifier.isPublic(cls.getModifiers());
    }

    /**
     * Converts the specified array of primitive Class objects to an array of its corresponding wrapper Class objects.
     *
     * @param classes the class array to convert, may be null or empty
     * @return an array which contains for each given class, the wrapper class or the original class if class is not a
     *         primitive. {@code null} if null input. Empty array if an empty array passed in.
     * @since 2.1
     */
    @SuppressWarnings("rawtypes")
    public static Class<?>[] primitivesToWrappers(final Class<?>... classes) {
        if (classes == null) {
            return null;
        }

        if (classes.length == 0) {
            return classes;
        }

        final Class<?>[] convertedClasses = new Class[classes.length];
        Arrays.setAll(convertedClasses, i -> primitiveToWrapper(classes[i]));
        return convertedClasses;
    }

    /**
     * Converts the specified primitive Class object to its corresponding wrapper Class object.
     *
     * <p>
     * NOTE: From v2.2, this method handles {@code Void.TYPE}, returning {@code Void.TYPE}.
     * </p>
     *
     * @param cls the class to convert, may be null
     * @return the wrapper class for {@code cls} or {@code cls} if {@code cls} is not a primitive. {@code null} if null
     *         input.
     * @since 2.1
     */
    public static Class<?> primitiveToWrapper(final Class<?> cls) {
        Class<?> convertedClass = cls;
        if (cls != null && cls.isPrimitive()) {
            convertedClass = PRIMITIVE_WRAPPER_MAP.get(cls);
        }
        return convertedClass;
    }

    /**
     * Converts an array of {@link Object} in to an array of {@link Class} objects. If any of these objects is null, a null
     * element will be inserted into the array.
     *
     * <p>
     * This method returns {@code null} for a {@code null} input array.
     * </p>
     *
     * @param array an {@link Object} array
     * @return a {@link Class} array, {@code null} if null array input
     * @since 2.4
     */
    @SuppressWarnings("rawtypes")
    public static Class<?>[] toClass(final Object... array) {
        if (array == null) {
            return null;
        }
        if (array.length == 0) {
            return ArrayUtils.EMPTY_CLASS_ARRAY;
        }
        final Class<?>[] classes = new Class[array.length];
        Arrays.setAll(classes, i -> array[i] == null ? null : array[i].getClass());
        return classes;
    }

    /**
     * Decides if the part that was just copied to its destination location in the work array can be kept as it was copied
     * or must be abbreviated. It must be kept when the part is the last one, which is the simple name of the class. In this
     * case the {@code source} index, from where the characters are copied points one position after the last character,
     * a.k.a. {@code source ==
     * originalLength}
     *
     * <p>
     * If the part is not the last one then it can be kept unabridged if the number of the characters copied so far plus the
     * character that are to be copied is less than or equal to the desired length.
     * </p>
     *
     * @param runAheadTarget the target index (where the characters were copied to) pointing after the last character copied
     *        when the current part was copied
     * @param source the source index (where the characters were copied from) pointing after the last character copied when
     *        the current part was copied
     * @param originalLength the original length of the class full name, which is abbreviated
     * @param desiredLength the desired length of the abbreviated class name
     * @return {@code true} if it can be kept in its original length {@code false} if the current part has to be abbreviated
     *         and
     */
    private static boolean useFull(final int runAheadTarget, final int source, final int originalLength, final int desiredLength) {
        return source >= originalLength || runAheadTarget + originalLength - source <= desiredLength;
    }

    /**
     * Converts the specified array of wrapper Class objects to an array of its corresponding primitive Class objects.
     *
     * <p>
     * This method invokes {@code wrapperToPrimitive()} for each element of the passed in array.
     * </p>
     *
     * @param classes the class array to convert, may be null or empty
     * @return an array which contains for each given class, the primitive class or <strong>null</strong> if the original class is not
     *         a wrapper class. {@code null} if null input. Empty array if an empty array passed in.
     * @see #wrapperToPrimitive(Class)
     * @since 2.4
     */
    @SuppressWarnings("rawtypes")
    public static Class<?>[] wrappersToPrimitives(final Class<?>... classes) {
        if (classes == null) {
            return null;
        }

        if (classes.length == 0) {
            return classes;
        }

        final Class<?>[] convertedClasses = new Class[classes.length];
        Arrays.setAll(convertedClasses, i -> wrapperToPrimitive(classes[i]));
        return convertedClasses;
    }

    /**
     * Converts the specified wrapper class to its corresponding primitive class.
     *
     * <p>
     * This method is the counter part of {@code primitiveToWrapper()}. If the passed in class is a wrapper class for a
     * primitive type, this primitive type will be returned (e.g. {@code Integer.TYPE} for {@code Integer.class}). For other
     * classes, or if the parameter is <strong>null</strong>, the return value is <strong>null</strong>.
     * </p>
     *
     * @param cls the class to convert, may be <strong>null</strong>
     * @return the corresponding primitive type if {@code cls} is a wrapper class, <strong>null</strong> otherwise
     * @see #primitiveToWrapper(Class)
     * @since 2.4
     */
    public static Class<?> wrapperToPrimitive(final Class<?> cls) {
        return WRAPPER_PRIMITIVE_MAP.get(cls);
    }

    /**
     * ClassUtils instances should NOT be constructed in standard programming. Instead, the class should be used as
     * {@code ClassUtils.getShortClassName(cls)}.
     *
     * <p>
     * This constructor is public to permit tools that require a JavaBean instance to operate.
     * </p>
     *
     * @deprecated TODO Make private in 4.0.
     */
    @Deprecated
    public ClassUtils() {
        // empty
    }

}