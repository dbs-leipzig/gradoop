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

import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;

/**
 * Represents configurations required to a Hazelcast based Gradoop
 * distributed cache.
 */
class HazelcastCacheClientConfiguration
  implements DistributedCacheClientConfiguration {

  /**
   * Cache server IP address.
   */
  private final String serverAddress;

  /**
   * Constructor.
   *
   * @param serverAddress cache server IP address
   */
  HazelcastCacheClientConfiguration(String serverAddress) {
    this.serverAddress = serverAddress;
  }

  @Override
  public String getServerAddress() {
    return serverAddress;
  }


  @Override
  public String toString() {
    return "@" + serverAddress;
  }
}
