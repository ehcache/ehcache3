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

import org.ehcache.javadoc.PublicApi;

import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.security.AccessController.doPrivileged;

@PublicApi
public class ParsingUtil {

  private static final Pattern SYSPROP = Pattern.compile("\\$\\{(?<property>[^{}]+)}");
  private static final Pattern PADDED_SYSPROP = Pattern.compile("\\s*" + SYSPROP.pattern() + "\\s*");

  public static String parsePropertyOrString(String s) {
    Matcher matcher = PADDED_SYSPROP.matcher(s);
    if (matcher.matches()) {
      String property = matcher.group("property");
      String value = doPrivileged((PrivilegedAction<String>) () -> System.getProperty(property));
      if (value == null) {
        throw new IllegalStateException(String.format("Replacement for ${%s} not found!", property));
      } else {
        return value;
      }
    } else {
      return s;
    }
  }

  public static BigInteger parsePropertyOrInteger(String s) {
    return new BigInteger(parsePropertyOrString(s));
  }

  public static BigInteger parsePropertyOrPositiveInteger(String s) {
    BigInteger value = parsePropertyOrInteger(s);
    if (value.compareTo(BigInteger.ZERO) > 0) {
      return value;
    } else {
      throw new IllegalArgumentException("Value " + value + " is not a positive integer");
    }
  }

  public static BigInteger parsePropertyOrNonNegativeInteger(String s) {
    BigInteger value = parsePropertyOrInteger(s);
    if (value.compareTo(BigInteger.ZERO) >= 0) {
      return value;
    } else {
      throw new IllegalArgumentException("Value " + value + " is not a non-negative integer");
    }
  }

  public static String parseStringWithProperties(String s) {
    Matcher matcher = SYSPROP.matcher(s);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      final String property = matcher.group("property");
      final String value = doPrivileged((PrivilegedAction<String>) () -> System.getProperty(property));
      if (value == null) {
        throw new IllegalStateException(String.format("Replacement for ${%s} not found!", property));
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

}
