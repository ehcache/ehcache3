package org.ehcache.transactions.xa;

/**
 * @author Ludovic Orban
 */
public enum XAState {

  IN_DOUBT,
  COMMITTED,
  ROLLED_BACK,

}
