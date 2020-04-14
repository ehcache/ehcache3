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
package org.ehcache.clustered;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.function.Function;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class TestRetryerTest {

  @Test @SuppressWarnings("unchecked")
  public void testExceptionsSuppressProperly() throws InitializationError {
    Result result = new JUnitCore().run(new NoIgnoringRunner(RepeatedFailure.class));

    assertThat(result.getFailureCount(), is(1));

    Throwable exception = result.getFailures().get(0).getException();
    assertThat(exception, hasMessage(equalTo("Failed: 4")));
    assertThat(exception.getSuppressed(), array(hasMessage(equalTo("Failed: 3"))));
    assertThat(exception.getSuppressed()[0].getSuppressed(), array(hasMessage(equalTo("Failed: 2"))));
    assertThat(exception.getSuppressed()[0].getSuppressed()[0].getSuppressed(), array(hasMessage(equalTo("Failed: 1"))));
  }

  @Test @SuppressWarnings("unchecked")
  public void testNoRetryOnImmediateSuccess() throws InitializationError {
    Result result = new JUnitCore().run(new NoIgnoringRunner(ImmediateSuccess.class));

    assertTrue(result.wasSuccessful());
    assertThat(result.getFailureCount(), is(0));
    assertThat(result.getRunCount(), is(1));
  }

  @Test @SuppressWarnings("unchecked")
  public void testRetryAndPassOnEventualSuccess() throws InitializationError {
    Result result = new JUnitCore().run(new NoIgnoringRunner(EventualSuccess.class));

    assertTrue(result.wasSuccessful());
    assertThat(result.getFailureCount(), is(0));
    assertThat(result.getRunCount(), is(4));
  }

  @Ignore
  public static class RepeatedFailure {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = new TestRetryer<>(Function.identity(), emptySet(), 1, 2, 3, 4);

    @Test
    public void test() {
      Assert.fail("Failed: " + RETRYER.getInput());
    }
  }

  @Ignore
  public static class ImmediateSuccess {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = new TestRetryer<>(Function.identity(), emptySet(), 1, 2, 3, 4);

    @Test
    public void test() {}
  }

  @Ignore
  public static class EventualSuccess {

    @ClassRule @Rule
    public static TestRetryer<Integer, Integer> RETRYER = new TestRetryer<>(Function.identity(), emptySet(), 1, 2, 3, 4);

    @Test
    public void test() {
      Assert.assertThat(RETRYER.getInput(), is(4));
    }
  }

  private static class NoIgnoringRunner extends BlockJUnit4ClassRunner {

    public NoIgnoringRunner(Class<?> klass) throws InitializationError {
      super(klass);
    }

    @Override
    protected boolean isIgnored(FrameworkMethod child) {
      return false;
    }
  };
}
