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

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A serializable XID.
 *
 * @author Ludovic Orban
 */
public class SerializableXid implements Xid, Serializable {

  private final int formatId;
  private final byte[] globalTransactionId;
  private final byte[] branchQualifier;

  /**
   * Create a {@link SerializableXid}, copying the format ID, GTRID and BQUAL of an existing {@link Xid}.
   *
   * @param xid a {@link Xid} implementation.
   */
  public SerializableXid(Xid xid) {
    if (xid.getGlobalTransactionId() == null) {
      throw new NullPointerException();
    }
    if (xid.getBranchQualifier() == null) {
      throw new NullPointerException();
    }
    this.formatId = xid.getFormatId();
    this.globalTransactionId = xid.getGlobalTransactionId();
    this.branchQualifier = xid.getBranchQualifier();
  }

  public int getFormatId() {
    return formatId;
  }

  public byte[] getBranchQualifier() {
    return Arrays.copyOf(branchQualifier, branchQualifier.length);
  }

  public byte[] getGlobalTransactionId() {
    return Arrays.copyOf(globalTransactionId, globalTransactionId.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SerializableXid that = (SerializableXid) o;

    if (formatId != that.formatId) return false;
    if (!Arrays.equals(globalTransactionId, that.globalTransactionId)) return false;
    return Arrays.equals(branchQualifier, that.branchQualifier);
  }

  @Override
  public int hashCode() {
    int result = formatId;
    result = 31 * result + Arrays.hashCode(globalTransactionId);
    result = 31 * result + Arrays.hashCode(branchQualifier);
    return result;
  }

  @Override
  public String toString() {
    return "SerializableXid{" +
        "formatId=" + formatId +
        ", globalTxId=" + Arrays.toString(globalTransactionId) +
        ", branchQualifier=" + Arrays.toString(branchQualifier) +
        '}';
  }

}
