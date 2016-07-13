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

package org.ehcache.clustered.common.internal.exceptions;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.lang.reflect.Constructor;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.junit.Assert.assertThat;

/**
 * Foundation for tests on {@link ClusterException} subclasses.
 */
public abstract class BaseClusteredEhcacheExceptionTest<T extends ClusterException> {

  private final Class<T> testClass;

  protected BaseClusteredEhcacheExceptionTest(Class<T> testClass) {
    if (testClass == null) {
      throw new NullPointerException("testClass");
    }
    this.testClass = testClass;
  }

  /**
   * Creates a new {@code <T>} exception using the message text provided.
   * If {@code <T>} has no {@code T(String)} constructor, the test calling this method is
   * skipped using with an assumption failure.
   *
   * @param message the message text to apply to the exception
   *
   * @return a new {@code <T>} exception
   */
  private T create(String message) throws Exception {
    Constructor<T> constructor;
    try {
      constructor = testClass.getConstructor(String.class);
    } catch (NoSuchMethodException e) {
      throw new AssumptionViolatedException("No public " + testClass.getSimpleName() +"(String) constructor");
    }
    return constructor.newInstance(message);
  }

  /**
   * Creates a new {@code <T>} exception using the message text and cause provided.
   * If {@code <T>} has no {@code T(String, Throwable)} constructor, the test calling this method is
   * skipped using with an assumption failure.
   *
   * @param message the message text to apply to the exception
   * @param cause the {@code Throwable} to apply as the cause of the new exception
   *
   * @return a new {@code <T>} exception
   */
  private T create(String message, Throwable cause) throws Exception {
    Constructor<T> constructor;
    try {
      constructor = testClass.getConstructor(String.class, Throwable.class);
    } catch (NoSuchMethodException e) {
      throw new AssumptionViolatedException("No public " + testClass.getSimpleName() +"(String, Throwable) constructor");
    }
    return constructor.newInstance(message, cause);
  }

  /**
   * Creates a new {@code <T>} exception using the cause provided.
   * If {@code <T>} has no {@code T(Throwable)} constructor, the test calling this method is
   * skipped using with an assumption failure.
   *
   * @param cause the {@code Throwable} to apply as the cause of the new exception
   *
   * @return a new {@code <T>} exception
   */
  private T create(Throwable cause) throws Exception {
    Constructor<T> constructor;
    try {
      constructor = testClass.getConstructor(Throwable.class);
    } catch (NoSuchMethodException e) {
      throw new AssumptionViolatedException("No public " + testClass.getSimpleName() +"(Throwable) constructor");
    }
    return constructor.newInstance(cause);
  }

  @Test
  public void testHasServerVersionUID() throws Exception {
    testClass.getDeclaredField("serialVersionUID");
  }

  @Test
  public void testType() throws Exception {
    assertThat(testClass, is(typeCompatibleWith(ClusterException.class)));
  }

  @Test
  public final void ctorMessage() throws Exception {
    T baseException = this.create("message text");

    assertThat(baseException.getMessage(), is("message text"));
    assertThat(baseException.getCause(), is(nullValue()));

    checkWithClientStack(baseException);
  }

  @Test
  public final void ctorMessageThrowable() throws Exception {
    Throwable baseCause = new Throwable("base cause");
    T baseException = this.create("message text", baseCause);

    assertThat(baseException.getMessage(), is("message text"));
    assertThat(baseException.getCause(), is(baseCause));

    checkWithClientStack(baseException);
  }

  @Test
  public final void ctorThrowable() throws Exception {
    Throwable baseCause = new Throwable("base cause");
    T baseException = this.create(baseCause);

    assertThat(baseException.getMessage(), is(baseCause.toString()));
    assertThat(baseException.getCause(), is(baseCause));

    checkWithClientStack(baseException);
  }

  private void checkWithClientStack(T baseException) {
    ClusterException copyException = baseException.withClientStackTrace();
    assertThat(copyException, is(notNullValue()));
    assertThat(copyException, is(instanceOf(testClass)));
    assertThat(copyException.getMessage(), is(baseException.getMessage()));
    assertThat(copyException.getCause(), Matchers.<Throwable>is(baseException));
  }
}