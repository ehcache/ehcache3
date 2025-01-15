/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.integration.statistics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assume.assumeThat;

public abstract class AbstractCalculationTest {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Rule
  public TestWatcher watchman = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      log.error("Counters at time of assertion: " + counters(), e);
    }
  };

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  protected final ResourcePools resources;

  protected AbstractCalculationTest(ResourcePoolsBuilder poolBuilder) {
    this.resources = poolBuilder.build();
    SizedResourcePool heapResource = resources.getPoolForResource(ResourceType.Core.HEAP);
    if (heapResource != null && heapResource.getUnit() instanceof MemoryUnit) {
      assumeThat(parseInt(getProperty("java.specification.version").split("\\.")[0]), is(lessThan(16)));
    }
  }

  /**
   * Gives a printout of the current counters to show them in case of error
   *
   * @return a string of all the counters
   */
  protected abstract String counters();

  /**
   * Convenience method that creates a {@code Set} from a list of {@code Integer}
   *
   * @param ints the list of integers
   * @return a set container all integers
   */
  protected static Set<Integer> asSet(Integer... ints) {
    return new HashSet<>(Arrays.asList(ints));
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <T> AbstractObjectAssert<?, T> expect(T actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractCharSequenceAssert<?, String> expect(String actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static AbstractBooleanAssert<?> expect(boolean actual) {
    return assertThat(actual);
  }

  /**
   * A little wrapper over {@code assertThat} that just mention that this what we expect from the test. So if the
   * expectation fails, it's probably the test that is wrong, not the implementation.
   *
   * @param actual actual value
   * @return an AssertJ assertion
   */
  protected static <K, V> AbstractMapAssert<?, ? extends Map<K, V>, K, V> expect(Map<K, V> actual) {
    return assertThat(actual);
  }
}
