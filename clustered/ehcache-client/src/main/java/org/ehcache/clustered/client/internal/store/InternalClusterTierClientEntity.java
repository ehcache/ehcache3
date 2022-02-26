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

package org.ehcache.clustered.client.internal.store;

/*
 * Since this interface has been used historically as the client-side interface that
 * identifies a cluster-tier entity it must remain as **the** interface even though
 * it is empty. We could remove it and hack up the server entity service to accept
 * both variants but this seems like a cleaner and more future proof decision. This
 * way if we need to introduce any 'internal' methods we can.
 */
public interface InternalClusterTierClientEntity extends ClusterTierClientEntity {
}
