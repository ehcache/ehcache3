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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.ehcache.clustered.ChainUtils.readPayload;

public class Matchers {

  public static Matcher<Chain> matchesChain(Chain expected) {
    return new TypeSafeMatcher<Chain>() {
      @Override
      protected boolean matchesSafely(Chain item) {
        Iterator<Element> expectedIt = expected.iterator();
        Iterator<Element> itemIt = item.iterator();

        while (expectedIt.hasNext() && itemIt.hasNext()) {
          Element expectedNext = expectedIt.next();
          Element itemNext = itemIt.next();

          if (!expectedNext.getPayload().equals(itemNext.getPayload())) {
            return false;
          }
        }

        return !expectedIt.hasNext() && !itemIt.hasNext();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" a chain matching ").appendValue(expected);
      }
    };
  }

  public static Matcher<Chain> hasPayloads(long ... payloads) {
    return new TypeSafeMatcher<Chain>() {
      @Override
      protected boolean matchesSafely(Chain item) {
        Iterator<Element> elements = item.iterator();
        for (long payload : payloads) {
          if (readPayload(elements.next().getPayload()) != payload) {
            return false;
          }
        }
        return !elements.hasNext();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" a chain containing the payloads ").appendValueList("[", ", ", "]", payloads);
      }
    };
  }


  public static Matcher<Chain> sameSequenceAs(Chain original) {
    List<Long> sequenceNumbers = sequenceNumbersOf(original);

    return new TypeSafeMatcher<Chain>() {
      @Override
      protected boolean matchesSafely(Chain item) {
        return sequenceNumbers.equals(sequenceNumbersOf(item));
      }

      @Override
      public void describeTo(Description description) {
       description.appendValue("a chain with sequence numbers matching ").appendValue(original);
      }
    };
  }

  private static List<Long> sequenceNumbersOf(Chain chain) {
    List<Long> sequenceNumbers = new ArrayList<>(chain.length());
    for (Element element : chain) {
      sequenceNumbers.add(((SequencedElement) element).getSequenceNumber());
    }
    return sequenceNumbers;
  }

}
