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

package org.gradoop.common.cache.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
import org.gradoop.common.cache.api.DistributedCacheServer;

import java.util.Set;

/**
 * Gradoop distributed cache server implementation based on Hazelcast.
 */
public class HazelcastCacheServer implements DistributedCacheServer {

  /**
   * Configurations required to connect to the server.
   */
  private final DistributedCacheClientConfiguration cacheClientConfiguration;

  /**
   * Hazelcast server instance.
   */
  private final HazelcastInstance instance;

  /**
   * Constructor.
   *
   * @param serverAddress host server IP address
   */
  public HazelcastCacheServer(String serverAddress) {

    this.cacheClientConfiguration =
      new HazelcastCacheClientConfiguration(serverAddress);

    Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();

    if (instances.isEmpty()) {
      Config config = new Config();
      config.setProperty("hazelcast.logging.type", "none");
      this.instance = Hazelcast.newHazelcastInstance(config);
    } else {
      this.instance = instances.iterator().next();
    }
  }

  @Override
  public DistributedCacheClientConfiguration getCacheClientConfiguration() {
    return cacheClientConfiguration;
  }


  @Override
  public void shutdown() {
    instance.getCluster().shutdown();
  }

}
