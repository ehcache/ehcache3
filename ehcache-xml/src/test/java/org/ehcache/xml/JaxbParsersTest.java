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

package org.ehcache.xml;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.math.BigInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class JaxbParsersTest {

  private static final String PROPERTY_PREFIX = JaxbParsersTest.class.getName() + ":";
  @Rule public final TestName testName = new TestName();

  @Test
  public void testParsePropertyOrStringFromNullString() {
    assertThrows(NullPointerException.class, () -> JaxbParsers.parsePropertyOrString(null));
  }

  @Test
  public void testParsePropertyOrStringWithoutProperty() {
    assertThat(JaxbParsers.parsePropertyOrString("${foobar"), is("${foobar"));
    assertThat(JaxbParsers.parsePropertyOrString("foobar"), is("foobar"));
    assertThat(JaxbParsers.parsePropertyOrString("foobar}"), is("foobar}"));
    assertThat(JaxbParsers.parsePropertyOrString("$foobar"), is("$foobar"));
  }

  @Test
  public void testParsePropertyOrStringWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThat(JaxbParsers.parsePropertyOrString("${" + property + "}"), is("barfoo"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrStringWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> JaxbParsers.parsePropertyOrString("${" + property + "}"));
  }

  @Test
  public void testParsePropertyOrIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> JaxbParsers.parsePropertyOrInteger(null));
  }

  @Test
  public void testParsePropertyOrIntegerValidWithoutProperty() {
    assertThat(JaxbParsers.parsePropertyOrInteger("123"), is(BigInteger.valueOf(123)));
    assertThat(JaxbParsers.parsePropertyOrInteger("-123"), is(BigInteger.valueOf(-123)));
  }

  @Test
  public void testParsePropertyOrIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(JaxbParsers.parsePropertyOrInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
    System.setProperty(property, "-123");
    try {
      assertThat(JaxbParsers.parsePropertyOrInteger("${" + property + "}"), is(BigInteger.valueOf(-123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> JaxbParsers.parsePropertyOrInteger("${" + property + "}"));
  }


  @Test
  public void testParsePropertyOrPositiveIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger(null));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerValidWithoutProperty() {
    assertThat(JaxbParsers.parsePropertyOrPositiveInteger("123"), is(BigInteger.valueOf(123)));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerOutOfRangeWithoutProperty() {
    assertThrows(IllegalArgumentException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger("0"));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(JaxbParsers.parsePropertyOrPositiveInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerOutOfRangeWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "0");
    try {
      assertThrows(IllegalArgumentException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> JaxbParsers.parsePropertyOrPositiveInteger("${" + property + "}"));
  }

  @Test
  public void parsePropertyOrNonNegativeInteger() {
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger(null));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerValidWithoutProperty() {
    assertThat(JaxbParsers.parsePropertyOrNonNegativeInteger("123"), is(BigInteger.valueOf(123)));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerOutOfRangeWithoutProperty() {
    assertThrows(IllegalArgumentException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger("-1"));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(JaxbParsers.parsePropertyOrNonNegativeInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerOutOfRangeWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "-1");
    try {
      assertThrows(IllegalArgumentException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> JaxbParsers.parsePropertyOrNonNegativeInteger("${" + property + "}"));
  }

  @Test
  public void parseStringWithProperties() {
  }

  @Test
  public void testParseStringWithPropertiesFromNullString() {
    assertThrows(NullPointerException.class, () -> JaxbParsers.parseStringWithProperties(null));
  }

  @Test
  public void testParseStringWithPropertiesWithoutProperties() {
    assertThat(JaxbParsers.parseStringWithProperties("foo${bar"), is("foo${bar"));
    assertThat(JaxbParsers.parseStringWithProperties("foobar"), is("foobar"));
    assertThat(JaxbParsers.parseStringWithProperties("foo}bar"), is("foo}bar"));
    assertThat(JaxbParsers.parseStringWithProperties("foo$bar"), is("foo$bar"));
  }

  @Test
  public void testParseStringWithPropertiesWithProperties() {
    String foo = PROPERTY_PREFIX + testName.getMethodName() + ":foo";
    String bar = PROPERTY_PREFIX + testName.getMethodName() + ":bar";
    System.setProperty(foo, "foo");
    System.setProperty(bar, "bar");
    try {
      assertThat(JaxbParsers.parseStringWithProperties("start:${" + foo + "}:middle:${" + bar + "}:end"), is("start:foo:middle:bar:end"));
    } finally {
      System.clearProperty(foo);
      System.clearProperty(bar);
    }
  }

  @Test
  public void testParseStringWithPropertiesWithMissingProperty() {
    String foo = PROPERTY_PREFIX + testName.getMethodName() + ":foo";
    String bar = PROPERTY_PREFIX + testName.getMethodName() + ":bar";
    assertThat(System.getProperty(bar), is(nullValue()));
    System.setProperty(foo, "foo");
    try {
      assertThrows(IllegalStateException.class, () -> JaxbParsers.parseStringWithProperties("start:${" + foo + "}:middle:${" + bar + "}:end"));
    } finally {
      System.clearProperty(foo);
    }
  }
}
