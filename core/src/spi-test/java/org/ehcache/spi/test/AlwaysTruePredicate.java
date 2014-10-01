package org.ehcache.spi.test;

import org.ehcache.function.Predicate;

/**
 * @author Aurelien Broszniowski
 */

public class AlwaysTruePredicate<V> implements Predicate<V> {

  @Override
  public boolean test(final V argument) {
    return true;
  }
}
