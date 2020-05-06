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
package org.ehcache.testing;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runners.JUnit4;
import org.junit.runners.model.InitializationError;

import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class TestRetryerTest {

  @Test @SuppressWarnings("unchecked")
  public void testExceptionsSuppressProperly() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(RepeatedFailure.class));

    assertThat(result.getFailureCount(), is(1));

    Throwable exception = result.getFailures().get(0).getException();
    assertThat(exception, hasMessage(equalTo("Failed: 4")));
    assertThat(exception.getSuppressed(), array(hasMessage(equalTo("Failed: 3"))));
    assertThat(exception.getSuppressed()[0].getSuppressed(), array(hasMessage(equalTo("Failed: 2"))));
    assertThat(exception.getSuppressed()[0].getSuppressed()[0].getSuppressed(), array(hasMessage(equalTo("Failed: 1"))));
  }

  @Test
  public void testNoRetryOnImmediateSuccess() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(ImmediateSuccess.class));
    assertTrue(result.wasSuccessful());
    assertThat(result.getFailureCount(), is(0));
    assertThat(result.getRunCount(), is(1));
  }

  @Test
  public void testRetryAndPassOnEventualSuccess() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(EventualSuccess.class));

    assertTrue(result.wasSuccessful());
    assertThat(result.getFailureCount(), is(0));
    assertThat(result.getRunCount(), is(4));
  }

  @Test
  public void testMissingClassRuleAnnotationTriggersFailure() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(MissingClassRuleAnnotation.class));

    assertFalse(result.wasSuccessful());
    assertThat(result.getRunCount(), is(0));
    assertThat(result.getFailureCount(), is(1));
    Throwable throwable = result.getFailures().get(0).getException();
    assertThat(throwable, hasMessage(is("TestRetryer must be annotated with both @ClassRule and @Rule")));
  }

  @Test
  public void testMissingRuleAnnotationTriggersFailure() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(MissingRuleAnnotation.class));

    assertFalse(result.wasSuccessful());
    assertThat(result.getRunCount(), is(1));
    assertThat(result.getFailureCount(), is(1));
    Throwable throwable = result.getFailures().get(0).getException();
    assertThat(throwable, hasMessage(is("TestRetryer must be annotated with both @ClassRule and @Rule")));
  }

  @Test
  public void testEffectivelyEmptyTestIsSafe() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(EffectivelyEmptyTest.class));

    assertTrue(result.wasSuccessful());
    assertThat(result.getIgnoreCount(), is(1));
    assertThat(result.getRunCount(), is(0));
    assertThat(result.getFailureCount(), is(0));
  }

  @Test
  public void testFailingTestLogsCorrectly() throws InitializationError {
    Result result = new JUnitCore().run(new JUnit4(PartiallyFailingThenPassingTest.class));

    assertTrue(result.wasSuccessful());
    assertThat(result.getFailureCount(), is(0));
    assertThat(result.getRunCount(), is(6));
  }

  @Ignore
  public static class RepeatedFailure {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void test() {
      Assert.fail("Failed: " + RETRYER.get());
    }
  }

  @Ignore
  public static class ImmediateSuccess {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void test() {}
  }

  @Ignore
  public static class EventualSuccess {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void test() {
      Assert.assertThat(RETRYER.get(), is(4));
    }
  }

  @Ignore
  public static class MissingClassRuleAnnotation {

    @Rule
    public TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void test() {
    }
  }

  @Ignore
  public static class MissingRuleAnnotation {

    @ClassRule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void test() {
    }
  }

  @Ignore
  public static class EffectivelyEmptyTest {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test @Ignore
    public void test() {
    }
  }

  @Ignore
  public static class PartiallyFailingThenPassingTest {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = TestRetryer.tryValues(1, 2, 3, 4);

    @Test
    public void passingTest() {
    }

    @Test
    public void failingTest() {
      assertThat(RETRYER.get(), is(3));
    }
  }
}
