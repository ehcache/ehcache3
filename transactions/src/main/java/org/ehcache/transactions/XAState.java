package org.ehcache.transactions;

/**
 * @author Ludovic Orban
 */
public enum XAState {

  IN_DOUBT,
  COMMITTED,
  ROLLED_BACK,

}
