/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.cache;

import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
import org.gradoop.common.cache.api.DistributedCacheServer;
import org.gradoop.common.cache.hazelcast.HazelCastCacheClient;
import org.gradoop.common.cache.hazelcast.HazelcastCacheServer;
import org.gradoop.common.util.NetworkHelper;

/**
 * Class providing factory methods to create distributed cache servers and
 * clients.
 */
public class DistributedCache {

  /**
   * Server factory method.
   *
   * @return cache server
   */
  public static DistributedCacheServer getServer() {
    return new HazelcastCacheServer(NetworkHelper.getLocalHost());
  }

  /**
   * Client factory method.
   *
   * @param cacheClientConfiguration configurations required to connect to
   *                                 the server
   * @param session session identifier
   *
   * @return cache client
   *
   * @throws InterruptedException
   */
  public static DistributedCacheClient getClient(
    DistributedCacheClientConfiguration cacheClientConfiguration,
    String session) throws InterruptedException {

    return new HazelCastCacheClient(cacheClientConfiguration, session);
  }
}
