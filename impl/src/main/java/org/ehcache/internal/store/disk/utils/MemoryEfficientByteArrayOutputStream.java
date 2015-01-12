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

package org.ehcache.internal.store.disk.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class is designed to minimise the number of System.arraycopy(); methods
 * required to complete.
 *
 * ByteArrayOutputStream in the JDK is tuned for a wide variety of purposes. This sub-class
 * starts with an initial size which is a closer match for ehcache usage.
 * 
 * @author <a href="mailto:gluck@gregluck.com">Greg Luck</a>
 * @version $Id$
 */
public final class MemoryEfficientByteArrayOutputStream extends ByteArrayOutputStream {

    /**
     * byte[] payloads are not expected to be tiny.
     */
    private static final int BEST_GUESS_SIZE = 512;

    private static int lastSize = BEST_GUESS_SIZE;

    /**
     * Creates a new byte array output stream, with a buffer capacity of
     * the specified size, in bytes.
     *
     * @param size the initial size.
     */
    public MemoryEfficientByteArrayOutputStream(int size) {
        super(size);
    }




    /**
     * Gets the bytes.
     *
     * @return the underlying byte[], or a copy if the byte[] is oversized
     */
    public synchronized byte getBytes()[] {
        if (buf.length == size()) {
            return buf;
        } else {
            byte[] copy = new byte[size()];
            System.arraycopy(buf, 0, copy, 0, size());
            return copy;
        }
    }

    /**
     * Factory method
     * @param serializable any Object that implements Serializable
     * @param estimatedPayloadSize how many bytes is expected to be in the Serialized representation
     * @return a ByteArrayOutputStream with a Serialized object in it
     * @throws java.io.IOException if something goes wrong with the Serialization
     */
    public static MemoryEfficientByteArrayOutputStream serialize(Serializable serializable, int estimatedPayloadSize) throws IOException {
        MemoryEfficientByteArrayOutputStream outstr = new MemoryEfficientByteArrayOutputStream(estimatedPayloadSize);
        ObjectOutputStream objstr = new ObjectOutputStream(outstr);
        objstr.writeObject(serializable);
        objstr.close();
        return outstr;
    }

    /**
     * Factory method. This method optimises memory by trying to make a better guess than the Java default
     * of 32 bytes by assuming the starting point for the serialized size will be what it was last time
     * this method was called.
     * @param serializable any Object that implements Serializable
     * @return a ByteArrayOutputStream with a Serialized object in it
     * @throws java.io.IOException if something goes wrong with the Serialization
     */
    public static MemoryEfficientByteArrayOutputStream serialize(Serializable serializable) throws IOException {
        MemoryEfficientByteArrayOutputStream outstr = new MemoryEfficientByteArrayOutputStream(lastSize);
        ObjectOutputStream objstr = new ObjectOutputStream(outstr);
        objstr.writeObject(serializable);
        objstr.close();
        lastSize = outstr.getBytes().length;
        return outstr;
    }
}
