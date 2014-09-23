package org.ehcache.spi.test;

import java.util.ArrayList;
import java.util.List;

public class Result {
  private long                startTime;
  private long                endTime;
  private long                runTime;
  private int                 runCount;
  private final List<Failure> failures = new ArrayList<Failure>();

  public Result() {
    //
  }

  public int getRunCount() {
    return runCount;
  }

  public int getFailureCount() {
    return failures.size();
  }

  public void testRunStarted() {
    startTime = System.nanoTime();
  }

  public void testRunFinished() {
    endTime = System.nanoTime();
    runTime = (endTime - startTime) / 1000L;
  }

  public long getRunTime() {
    return runTime;
  }

  public void testFinished() {
    runCount++;
  }

  public void testFailure(Failure failure) {
    failures.add(failure);
  }

  public List<Failure> getFailures() {
    return failures;
  }

  public boolean wasSuccessful() {
    return failures.size() == 0;
  }

  public void reportAndThrow() throws Exception {
    System.out.println("* " + (getRunCount() + failures.size()) + " tests ran, took "
        + getRunTime() + " ms. " + getRunCount() + " successful. " + failures.size() + " failed.");
    if (!wasSuccessful()) {
      for (Failure failure : failures) {
        System.out.println("*** " + failure.getTestMethod() + " failed: "
            + failure.getThrownException().getMessage());
        System.out.println(failure.getTrace());
        System.out.println("*********************************");
      }
      throw new Exception(failures.get(0).getThrownException());
    }
  }
}
