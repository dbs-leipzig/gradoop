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

import java.util.List;
import java.util.Map;

/**
 * Describes a distributed cache client.
 */
public interface DistributedCacheClient {

  /**
   * Returns a cached list.
   *
   * @param name list name
   * @param <T> list element type
   *
   * @return cached list
   */
  <T> List<T> getList(String name);

  /**
   * Sets a cached list.
   *
   * @param name list name
   * @param list list
   * @param <T> list elelement type
   */
  <T> void setList(String name, List<T> list);

  /**
   * Returns a cached map.
   *
   * @param name map name
   * @param <K> map key type
   * @param <V> map value type
   *
   * @return cached map
   */
  <K, V> Map<K, V> getMap(String name);

  /**
   * Sets a cached list.
   *
   * @param name list name
   * @param map map
   * @param <K> map key type
   * @param <V> map value type
   */
  <K, V> void setMap(String name, Map<K, V> map);

  /**
   * Returns the value of a counter
   * @param name counter name
   * @return current count
   */
  long getCounter(String name);

  /**
   * Sets a counter to given value.
   * @param name counter name
   * @param count value
   */
  void setCounter(String name, long count);

  /**
   * Increments a counter by 1 and returns its new value.
   *
   * @param name counter name
   * @return incremented value
   */
  long incrementAndGetCounter(String name);

  /**
   * Increments a counter by a given value and returns its new value.
   *
   * @param name counter name
   * @param count value to increment
   */
  void addAndGetCounter(String name, long count);

  /**
   * Waits for a counter to reach a certain value.
   *
   * @param name counter name
   * @param count value to reach
   *
   * @throws InterruptedException
   */
  void waitForCounterToReach(
    String name, int count) throws InterruptedException;

  /**
   * Resets a counter to 0.
   *
   * @param name counter name
   */
  void resetCounter(String name);

  /**
   * Triggers an event.
   *
   * @param name event name
   *
   * @throws InterruptedException
   */
  void triggerEvent(String name) throws InterruptedException;

  /**
   * Waits for an event to be triggered.
   *
   * @param name event name
   *
   * @throws InterruptedException
   */
  void waitForEvent(String name) throws InterruptedException;

  /**
   * Deletes a cached object.
   *
   * @param name object name
   */
  void delete(String name);

}
