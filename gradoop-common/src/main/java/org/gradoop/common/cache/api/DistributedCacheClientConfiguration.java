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

package org.gradoop.common.cache.api;

import java.io.Serializable;

/**
 * Describes the configurations required to connect to a distributed cache
 * server.
 */
public interface DistributedCacheClientConfiguration extends Serializable {
  /**
   * Returns the cache server's IP address.
   *
   * @return IP address
   */
  String getServerAddress();
}
