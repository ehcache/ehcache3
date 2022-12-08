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

package org.ehcache.spi.test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Hung Huynh
 */
public class Result {
  private       long              startTime;
  private       long              runTime;
  private       int               runCount;
  private final List<ResultState> failedTests  = new ArrayList<>();
  private final List<ResultState> skippedTests = new ArrayList<>();
  private final List<ResultState> testsWithLegalException = new ArrayList<>();

  public Result() {
    //
  }

  public int getRunCount() {
    return runCount;
  }

  public int getFailureCount() {
    return failedTests.size();
  }

  public void testRunStarted() {
    startTime = System.nanoTime();
  }

  public void testRunFinished() {
    long endTime = System.nanoTime();
    runTime = (endTime - startTime) / 1000L;
  }

  public long getRunTime() {
    return runTime;
  }

  public void testFinished() {
    runCount++;
  }

  public void testFailed(ResultState failedTest) {
    failedTests.add(failedTest);
  }

  public void testSkipped(ResultState skippedTest) {
    skippedTests.add(skippedTest);
  }

  public void testsOverlookedDueToLegalException(ResultState legalException) {
    testsWithLegalException.add(legalException);
  }

  public List<ResultState> getFailedTests() {
    return failedTests;
  }

  public boolean wasSuccessful() {
    return failedTests.isEmpty();
  }

  public void reportAndThrow() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("***> SPITester: ")
        .append(getTotalRunCount()).append(" tests ran, ")
        .append("took ").append(runtimeAsString()).append(". ")
        .append("Passed: ").append(getRunCount()).append(". ")
        .append("Overlooked due to legal exception: ").append(getTestsWithLegalException().size()).append(". ")
        .append("Skipped: ").append(skippedTests.size()).append(". ")
        .append("Failed: ").append(failedTests.size());
    System.out.println(sb.toString());

    for (ResultState skippedTest : skippedTests) {
      sb = new StringBuilder();
      sb.append("* ").append(skippedTest.getName())
          .append(" skipped: ").append(skippedTest.getReason());
      System.out.println(sb.toString());
    }

    for(ResultState overlookedDueToLegalException : testsWithLegalException) {
      sb = new StringBuilder();
      sb.append("* ").append(overlookedDueToLegalException.getName())
          .append(" overlooked due to legal exception: ").append(overlookedDueToLegalException.getReason());
      System.out.println(sb.toString());
    }

    if (!wasSuccessful()) {
      System.out.println();
      for (ResultState failure : failedTests) {
        sb = new StringBuilder();
        sb.append("* ").append(failure.getName())
            .append(" failed: ").append(failure.getReason())
            .append(System.getProperty("line.separator"));
        System.out.println(sb.toString());
      }
      throw new Exception(failedTests.get(0).getReason());
    }
  }

  private int getTotalRunCount() {
    return getRunCount() + skippedTests.size()+failedTests.size() + testsWithLegalException.size();
  }

  private String runtimeAsString() {
    return (runTime > 1000) ? String.format("%.1f s", (runTime / 1000.0)) : runTime + " ms";
  }

  public List<ResultState> getTestsWithLegalException() {
    return testsWithLegalException;
  }
}
