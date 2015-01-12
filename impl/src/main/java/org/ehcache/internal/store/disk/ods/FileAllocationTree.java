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
package org.ehcache.internal.store.disk.ods;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File allocation tree allows C-like alloc/free operations on a random access file.
 *
 * @author Chris Dennis
 */
public final class FileAllocationTree extends RegionSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileAllocationTree.class);
    
    private long fileSize;
    private final RandomAccessFile data;

    /**
     * Create a file allocation tree for the given file, capping it's size at maxSize.
     */
    public FileAllocationTree(long maxSize, RandomAccessFile file) {
        super(maxSize);
        this.data = file;
    }

    /**
     * Allocate a new region of the given size.
     */
    public synchronized Region alloc(long size) {
        Region r = find(size);
        mark(r);
        return r;
    }

    /**
     * Mark this region as used
     */
    public synchronized void mark(Region r) {
        Region current = removeAndReturn(Long.valueOf(r.start()));
        if (current == null) {
            throw new IllegalArgumentException();
        }
        Region newRange = current.remove(r);
        if (newRange != null) {
            add(current);
            add(newRange);
        } else if (!current.isNull()) {
            add(current);
        }
        checkGrow(r);
    }

    /**
     * Mark this region as free.
     */
    public synchronized void free(Region r) {
        // Step 1 : Check if the previous number is present, if so add to the same Range.
        Region prev = removeAndReturn(Long.valueOf(r.start() - 1));
        if (prev != null) {
            prev.merge(r);
            Region next = removeAndReturn(Long.valueOf(r.end() + 1));
            if (next != null) {
                prev.merge(next);
            }
            add(prev);
            checkShrink(prev);
            return;
        }

        // Step 2 : Check if the next number is present, if so add to the same Range.
        Region next = removeAndReturn(Long.valueOf(r.end() + 1));
        if (next != null) {
            next.merge(r);
            add(next);
            checkShrink(next);
            return;
        }

        // Step 3: Add a new range for just this number.
        add(r);
        checkShrink(r);
    }

    /**
     * Mark this whole file as free
     */
    @Override
    public synchronized void clear() {
        super.clear();
    }

    private void checkGrow(Region alloc) {
        if (alloc.end() >= fileSize) {
            fileSize = alloc.end() + 1;
            grow(fileSize);
        }
    }

    private void checkShrink(Region free) {
        if (free.end() >= fileSize - 1) {
            fileSize = free.start();
            shrink(fileSize);
        }
    }

    private void grow(long size) {
        //no-op
    }

    private void shrink(long size) {
        if (data == null) {
            return;
        } else {
            synchronized (data) {
                try {
                    data.setLength(size);
                } catch (IOException e) {
                    LOGGER.info("Exception while trying to shrink file", e);
                }
            }
        }
    }

    /**
     * Return the current occupied size of this file.
     */
    public synchronized long getFileSize() {
        return fileSize;
    }
}
