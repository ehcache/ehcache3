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

import org.ehcache.xml.spi.ParsingUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.math.BigInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class ParsingUtilTest {

  private static final String PROPERTY_PREFIX = ParsingUtilTest.class.getName() + ":";
  @Rule public final TestName testName = new TestName();

  @Test
  public void testParsePropertyOrStringFromNullString() {
    assertThrows(NullPointerException.class, () -> ParsingUtil.parsePropertyOrString(null));
  }

  @Test
  public void testParsePropertyOrStringWithoutProperty() {
    assertThat(ParsingUtil.parsePropertyOrString("${foobar"), is("${foobar"));
    assertThat(ParsingUtil.parsePropertyOrString("foobar"), is("foobar"));
    assertThat(ParsingUtil.parsePropertyOrString("foobar}"), is("foobar}"));
    assertThat(ParsingUtil.parsePropertyOrString("$foobar"), is("$foobar"));
  }

  @Test
  public void testParsePropertyOrStringWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThat(ParsingUtil.parsePropertyOrString("${" + property + "}"), is("barfoo"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrStringWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> ParsingUtil.parsePropertyOrString("${" + property + "}"));
  }

  @Test
  public void testParsePropertyOrIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> ParsingUtil.parsePropertyOrInteger(null));
  }

  @Test
  public void testParsePropertyOrIntegerValidWithoutProperty() {
    assertThat(ParsingUtil.parsePropertyOrInteger("123"), is(BigInteger.valueOf(123)));
    assertThat(ParsingUtil.parsePropertyOrInteger("-123"), is(BigInteger.valueOf(-123)));
  }

  @Test
  public void testParsePropertyOrIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(ParsingUtil.parsePropertyOrInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
    System.setProperty(property, "-123");
    try {
      assertThat(ParsingUtil.parsePropertyOrInteger("${" + property + "}"), is(BigInteger.valueOf(-123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> ParsingUtil.parsePropertyOrInteger("${" + property + "}"));
  }


  @Test
  public void testParsePropertyOrPositiveIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger(null));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerValidWithoutProperty() {
    assertThat(ParsingUtil.parsePropertyOrPositiveInteger("123"), is(BigInteger.valueOf(123)));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerOutOfRangeWithoutProperty() {
    assertThrows(IllegalArgumentException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger("0"));
  }

  @Test
  public void testParsePropertyOrPositiveIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(ParsingUtil.parsePropertyOrPositiveInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerOutOfRangeWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "0");
    try {
      assertThrows(IllegalArgumentException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrPositiveIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> ParsingUtil.parsePropertyOrPositiveInteger("${" + property + "}"));
  }

  @Test
  public void parsePropertyOrNonNegativeInteger() {
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerFromNullString() {
    assertThrows(NullPointerException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger(null));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerValidWithoutProperty() {
    assertThat(ParsingUtil.parsePropertyOrNonNegativeInteger("123"), is(BigInteger.valueOf(123)));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerInvalidWithoutProperty() {
    assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger("foobar"));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerOutOfRangeWithoutProperty() {
    assertThrows(IllegalArgumentException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger("-1"));
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerValidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "123");
    try {
      assertThat(ParsingUtil.parsePropertyOrNonNegativeInteger("${" + property + "}"), is(BigInteger.valueOf(123)));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerInvalidWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "barfoo");
    try {
      assertThrows(NumberFormatException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerOutOfRangeWithProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    System.setProperty(property, "-1");
    try {
      assertThrows(IllegalArgumentException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger("${" + property + "}"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testParsePropertyOrNonNegativeIntegerWithMissingProperty() {
    String property = PROPERTY_PREFIX + testName.getMethodName();
    assertThrows(IllegalStateException.class, () -> ParsingUtil.parsePropertyOrNonNegativeInteger("${" + property + "}"));
  }

  @Test
  public void parseStringWithProperties() {
  }

  @Test
  public void testParseStringWithPropertiesFromNullString() {
    assertThrows(NullPointerException.class, () -> ParsingUtil.parseStringWithProperties(null));
  }

  @Test
  public void testParseStringWithPropertiesWithoutProperties() {
    assertThat(ParsingUtil.parseStringWithProperties("foo${bar"), is("foo${bar"));
    assertThat(ParsingUtil.parseStringWithProperties("foobar"), is("foobar"));
    assertThat(ParsingUtil.parseStringWithProperties("foo}bar"), is("foo}bar"));
    assertThat(ParsingUtil.parseStringWithProperties("foo$bar"), is("foo$bar"));
  }

  @Test
  public void testParseStringWithPropertiesWithProperties() {
    String foo = PROPERTY_PREFIX + testName.getMethodName() + ":foo";
    String bar = PROPERTY_PREFIX + testName.getMethodName() + ":bar";
    System.setProperty(foo, "foo");
    System.setProperty(bar, "bar");
    try {
      assertThat(ParsingUtil.parseStringWithProperties("start:${" + foo + "}:middle:${" + bar + "}:end"), is("start:foo:middle:bar:end"));
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
      assertThrows(IllegalStateException.class, () -> ParsingUtil.parseStringWithProperties("start:${" + foo + "}:middle:${" + bar + "}:end"));
    } finally {
      System.clearProperty(foo);
    }
  }
}
