package org.ehcache.spi.test;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Failure {
  private final String    testMethod;
  private final Throwable thrownException;

  public Failure(String testMethod, Throwable thrownException) {
    this.testMethod = testMethod;
    this.thrownException = thrownException;
  }

  public String getTestMethod() {
    return testMethod;
  }

  public Throwable getThrownException() {
    return thrownException;
  }

  public String getTrace() {
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    thrownException.printStackTrace(writer);
    StringBuffer buffer = stringWriter.getBuffer();
    return buffer.toString();
  }

}
