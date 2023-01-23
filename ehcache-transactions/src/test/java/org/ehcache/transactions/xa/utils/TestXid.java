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
package org.ehcache.transactions.xa.utils;

import javax.transaction.xa.Xid;
import java.util.Arrays;

/**
 * @author Ludovic Orban
 */
public class TestXid implements Xid {

  private int formatId = 123456;
  private byte[] gtrid;
  private byte[] bqual;

  public TestXid(long gtrid, long bqual) {
    this.gtrid = longToBytes(gtrid);
    this.bqual = longToBytes(bqual);
  }

  public int getFormatId() {
    return formatId;
  }

  public byte[] getGlobalTransactionId() {
    return gtrid;
  }

  public byte[] getBranchQualifier() {
    return bqual;
  }

  @Override
  public int hashCode() {
    return formatId + arrayHashCode(gtrid) + arrayHashCode(bqual);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TestXid) {
      TestXid otherXid = (TestXid) o;
      return formatId == otherXid.formatId &&
          Arrays.equals(gtrid, otherXid.gtrid) &&
          Arrays.equals(bqual, otherXid.bqual);
    }
    return false;
  }

  @Override
  public String toString() {
    return "TestXid [" + Arrays.toString(gtrid) + " " + Arrays.toString(bqual) + "]";
  }

  private static int arrayHashCode(byte[] uid) {
    int hash = 0;
    for (int i = uid.length - 1; i > 0; i--) {
      hash <<= 1;

      if (hash < 0) {
        hash |= 1;
      }

      hash ^= uid[i];
    }
    return hash;
  }

  public static byte[] longToBytes(long aLong) {
    byte[] array = new byte[8];

    array[7] = (byte) (aLong & 0xff);
    array[6] = (byte) ((aLong >> 8) & 0xff);
    array[5] = (byte) ((aLong >> 16) & 0xff);
    array[4] = (byte) ((aLong >> 24) & 0xff);
    array[3] = (byte) ((aLong >> 32) & 0xff);
    array[2] = (byte) ((aLong >> 40) & 0xff);
    array[1] = (byte) ((aLong >> 48) & 0xff);
    array[0] = (byte) ((aLong >> 56) & 0xff);

    return array;
  }

}
