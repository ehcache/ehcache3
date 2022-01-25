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
package org.ehcache.core.statistics;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.EnumSet.of;

public class TierOperationOutcomes {

  public static final Map<GetOutcome, Set<StoreOperationOutcomes.GetOutcome>> GET_TRANSLATION;

  static {
    Map<GetOutcome, Set<StoreOperationOutcomes.GetOutcome>> translation = new EnumMap<GetOutcome, Set<StoreOperationOutcomes.GetOutcome>>(GetOutcome.class);
    translation.put(GetOutcome.HIT, of(StoreOperationOutcomes.GetOutcome.HIT));
    translation.put(GetOutcome.MISS, of(StoreOperationOutcomes.GetOutcome.MISS, StoreOperationOutcomes.GetOutcome.TIMEOUT));
    GET_TRANSLATION = unmodifiableMap(translation);
  }

  public static final Map<GetOutcome, Set<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome>> GET_AND_FAULT_TRANSLATION;

  static {
    Map<GetOutcome, Set<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome>> translation = new EnumMap<GetOutcome, Set<AuthoritativeTierOperationOutcomes.GetAndFaultOutcome>>(GetOutcome.class);
    translation.put(GetOutcome.HIT, of(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.HIT));
    translation.put(GetOutcome.MISS, of(AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.MISS, AuthoritativeTierOperationOutcomes.GetAndFaultOutcome.TIMEOUT));
    GET_AND_FAULT_TRANSLATION = unmodifiableMap(translation);
  }

  public static final Map<GetOutcome, Set<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome>> GET_AND_REMOVE_TRANSLATION;

  static {
    Map<GetOutcome, Set<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome>> translation = new EnumMap<GetOutcome, Set<LowerCachingTierOperationsOutcome.GetAndRemoveOutcome>>(GetOutcome.class);
    translation.put(GetOutcome.HIT, of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    translation.put(GetOutcome.MISS, of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
    GET_AND_REMOVE_TRANSLATION = unmodifiableMap(translation);
  }

  public static final Map<GetOutcome, Set<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome>> GET_OR_COMPUTEIFABSENT_TRANSLATION;

  static {
    Map<GetOutcome, Set<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome>> translation = new EnumMap<GetOutcome, Set<CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome>>(GetOutcome.class);
    translation.put(GetOutcome.HIT, of(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.HIT));
    translation.put(GetOutcome.MISS, of(CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULTED, CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED,
      CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.FAULT_FAILED_MISS, CachingTierOperationOutcomes.GetOrComputeIfAbsentOutcome.MISS));
    GET_OR_COMPUTEIFABSENT_TRANSLATION = unmodifiableMap(translation);
  }

  public static final Map<EvictionOutcome, Set<StoreOperationOutcomes.EvictionOutcome>> EVICTION_TRANSLATION;

  static {
    Map<EvictionOutcome, Set<StoreOperationOutcomes.EvictionOutcome>> translation = new EnumMap<EvictionOutcome, Set<StoreOperationOutcomes.EvictionOutcome>>(EvictionOutcome.class);
    translation.put(EvictionOutcome.SUCCESS, of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
    translation.put(EvictionOutcome.FAILURE, of(StoreOperationOutcomes.EvictionOutcome.FAILURE));
    EVICTION_TRANSLATION = unmodifiableMap(translation);
  }

  public enum GetOutcome {
    HIT,
    MISS,
  }

  public enum EvictionOutcome {
    SUCCESS,
    FAILURE
  }

}
