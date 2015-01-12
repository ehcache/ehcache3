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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Various bits of black magic garnered from experts on the   concurrency-interest@cs.oswego.edu mailing list.
 *
 * @author Greg Luck
 * @version $Id: ConcurrencyUtil.java 7833 2013-07-25 01:00:20Z cschanck $
 */
public final class ConcurrencyUtil {

    private static final int DOUG_LEA_BLACK_MAGIC_OPERAND_1 = 20;
    private static final int DOUG_LEA_BLACK_MAGIC_OPERAND_2 = 12;
    private static final int DOUG_LEA_BLACK_MAGIC_OPERAND_3 = 7;
    private static final int DOUG_LEA_BLACK_MAGIC_OPERAND_4 = 4;


    /**
     * Utility class therefore precent construction
     */
    private ConcurrencyUtil() {
        //noop;
    }

    /**
     * Returns a hash code for non-null Object x.
     * <p/>
     * This function ensures that hashCodes that differ only by
     * constant multiples at each bit position have a bounded
     * number of collisions. (Doug Lea)
     *
     * @param object the object serving as a key
     * @return the hash code
     */
    public static int hash(Object object) {
        int h = object.hashCode();
        h ^= (h >>> DOUG_LEA_BLACK_MAGIC_OPERAND_1) ^ (h >>> DOUG_LEA_BLACK_MAGIC_OPERAND_2);
        return h ^ (h >>> DOUG_LEA_BLACK_MAGIC_OPERAND_3) ^ (h >>> DOUG_LEA_BLACK_MAGIC_OPERAND_4);
    }

    /**
     * Selects a lock for a key. The same lock is always used for a given key.
     *
     * @param key
     * @return the selected lock index
     */
    public static int selectLock(final Object key, int numberOfLocks) {
        int number = numberOfLocks & (numberOfLocks - 1);
        if (number != 0) {
            throw new IllegalArgumentException("Lock number must be a power of two: " + numberOfLocks);
        }
        if (key == null) {
            return 0;
        } else {
            int hash = hash(key) & (numberOfLocks - 1);
            return hash;
        }

    }


   /**
    * Properly shutdown and await pool termination for an arbitrary
    * amount of time.
    *
    * @param pool Pool to shutdown
    * @param waitSeconds Seconds to wait before throwing exception
    * @throws java.util.concurrent.TimeoutException Thrown if the pool does not shutdown in the specified time
    */
   public static void shutdownAndWaitForTermination(ExecutorService pool, int waitSeconds) throws TimeoutException {
      // shut it down
      pool.shutdown();
      try {
         // wait, wait, wait
         if (!pool.awaitTermination(waitSeconds, TimeUnit.SECONDS)) {
            // things still running, nuke it
            pool.shutdownNow();
            // wait, wait, wai
            if (!pool.awaitTermination(waitSeconds, TimeUnit.SECONDS)) {
               // boo hiss, didn't shutdown
               throw new TimeoutException("Pool did not terminate");
            }
         }
      } catch (InterruptedException ie) {
         // try, try again
         pool.shutdownNow();
         Thread.currentThread().interrupt();
      }
   }
}
