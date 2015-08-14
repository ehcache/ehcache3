/**
 *  Copyright Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.ehcache.transactions;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A serializable XID
 *
 * @author Ludovic Orban
 */
public class SerializableXid implements Xid, Serializable {

    private final int formatId;
    private final byte[] globalTransactionId;
    private final byte[] branchQualifier;

    /**
     * Create a SerializableXid, copying the GTRID and BQUAL of an existing XID
     *
     * @param xid a SerializableXid
     */
    public SerializableXid(Xid xid) {
        this.formatId = xid.getFormatId();
        this.globalTransactionId = xid.getGlobalTransactionId();
        this.branchQualifier = xid.getBranchQualifier();
    }

    /**
     * {@inheritDoc}
     */
    public int getFormatId() {
        return formatId;
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof SerializableXid)) {
            return false;
        }

        SerializableXid otherXid = (SerializableXid) obj;
        return formatId == otherXid.getFormatId() &&
               Arrays.equals(globalTransactionId, otherXid.getGlobalTransactionId()) &&
               Arrays.equals(branchQualifier, otherXid.branchQualifier);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        int hashCode = formatId;
        if (globalTransactionId != null) {
            hashCode += Arrays.hashCode(globalTransactionId);
        }
        if (branchQualifier != null) {
            hashCode += Arrays.hashCode(branchQualifier);
        }
        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SerializableXid{" +
               "formatId=" + formatId +
               ", globalTxId=" + Arrays.toString(globalTransactionId) +
               ", branchQualifier=" + Arrays.toString(branchQualifier) +
               '}';
    }

}
