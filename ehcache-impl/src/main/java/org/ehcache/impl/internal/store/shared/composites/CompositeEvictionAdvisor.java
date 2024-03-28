package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.config.EvictionAdvisor;

import java.util.Map;

public class CompositeEvictionAdvisor implements EvictionAdvisor<CompositeValue<?>, CompositeValue<?>> {
  private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap;

  public CompositeEvictionAdvisor(Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap) {
    this.evictionAdvisorMap = evictionAdvisorMap;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean adviseAgainstEviction(CompositeValue<?> key, CompositeValue<?> value) {
    return ((EvictionAdvisor) evictionAdvisorMap.get(key.getStoreId())).adviseAgainstEviction(key.getValue(), value.getValue());
  }
}
