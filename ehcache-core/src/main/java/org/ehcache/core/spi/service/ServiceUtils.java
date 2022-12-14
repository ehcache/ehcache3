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

package org.ehcache.core.spi.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class to find a service or service configuration matching the wanted type. Note that the class
 * is named {@code ServiceUtils} but it would actually work with anything, not only service implementations.
 */
public final class ServiceUtils {

  private ServiceUtils() {
    // No instance possible
  }

  private static <T> Stream<T> findStreamAmongst(Class<T> clazz, Collection<?> instances) {
    return instances.stream()
      .filter(clazz::isInstance)
      .map(clazz::cast);
  }

  /**
   * Find instances of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instances
   * @return the list of compatible instances
   */
  public static <T> Collection<T> findAmongst(Class<T> clazz, Collection<?> instances) {
    return findStreamAmongst(clazz, instances)
      .collect(Collectors.toList());
  }

  /**
   * Find instances of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instances
   * @return the list of compatible instances
   */
  public static <T> Collection<T> findAmongst(Class<T> clazz, Object ... instances) {
    return findAmongst(clazz, Arrays.asList(instances));
  }

  /**
   * Find the only expected instance of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instance
   * @return the compatible instance or null if none are found
   * @throws IllegalArgumentException if more than one matching instance
   */
  public static <T> T findSingletonAmongst(Class<T> clazz, Collection<?> instances) {
    return findOptionalAmongst(clazz, instances)
      .orElse(null);
  }

  /**
   * Find the only expected instance of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instance
   * @return the optionally found compatible instance
   * @throws IllegalArgumentException if more than one matching instance
   */
  public static <T> Optional<T> findOptionalAmongst(Class<T> clazz, Collection<?> instances) {
    return findStreamAmongst(clazz, instances)
      .reduce((i1, i2) -> {
        throw new IllegalArgumentException("More than one " + clazz.getName() + " found");
      });
  }

  /**
   * Find the only expected instance of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instance
   * @return the compatible instance or null if none are found
   * @throws IllegalArgumentException if more than one matching instance
   */
  public static <T> T findSingletonAmongst(Class<T> clazz, Object ... instances) {
    return findSingletonAmongst(clazz, Arrays.asList(instances));
  }

  /**
   * Find the only expected instance of {@code clazz} among the {@code instances}.
   *
   * @param clazz searched class
   * @param instances instances looked at
   * @param <T> type of the searched instance
   * @return the optionally found compatible instance
   * @throws IllegalArgumentException if more than one matching instance
   */
  public static <T> Optional<T> findOptionalAmongst(Class<T> clazz, Object ... instances) {
    return findOptionalAmongst(clazz, Arrays.asList(instances));
  }
}
