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

package org.ehcache.transactions.xa.internal;

import org.ehcache.spi.copy.Copier;

/**
 * The {@link Copier} that is responsible for copying a {@link SoftLock} containing a value.
 *
 * @author Ludovic Orban
 */
class SoftLockValueCombinedCopier<T> implements Copier<SoftLock<T>> {

  private final Copier<T> valueCopier;

  SoftLockValueCombinedCopier(Copier<T> valueCopier) {
    this.valueCopier = valueCopier;
  }

  @Override
  public SoftLock<T> copyForRead(SoftLock<T> obj) {
    T oldValue = valueCopier.copyForRead(obj.getOldValue());
    XAValueHolder<T> valueHolder = obj.getNewValueHolder();
    XAValueHolder<T> newValueHolder = valueHolder == null ? null : new XAValueHolder<T>(valueHolder, valueCopier.copyForRead(valueHolder.value()));
    return new SoftLock<T>(obj.getTransactionId(), oldValue, newValueHolder);
  }

  @Override
  public SoftLock<T> copyForWrite(SoftLock<T> obj) {
    T oldValue = valueCopier.copyForWrite(obj.getOldValue());
    XAValueHolder<T> valueHolder = obj.getNewValueHolder();
    XAValueHolder<T> newValueHolder = valueHolder == null ? null : new XAValueHolder<T>(valueHolder, valueCopier.copyForWrite(valueHolder.value()));
    return new SoftLock<T>(obj.getTransactionId(), oldValue, newValueHolder);
  }

}
