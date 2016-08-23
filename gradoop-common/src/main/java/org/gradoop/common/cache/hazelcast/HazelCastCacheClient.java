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

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Gradoop distributed cache client implementation based on Hazelcast.
 */
public class HazelCastCacheClient implements DistributedCacheClient {

  /**
   * Hazelcast client instance.
   */
  private final HazelcastInstance instance;

  /**
   * Session identifier.
   */
  private final String session;

  /**
   * Constructor.
   *
   * @param cacheClientConfiguration configuration required for server
   *                                 connection.
   * @param session session identifier
   */
  public HazelCastCacheClient(
    DistributedCacheClientConfiguration cacheClientConfiguration,
    String session) {

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig().setAddresses(
        Lists.newArrayList(cacheClientConfiguration.getServerAddress()));
    clientConfig.setProperty("hazelcast.logging.type", "none");

    this.session = session;

    Collection<HazelcastInstance> instances =
      HazelcastClient.getAllHazelcastClients();

    if (instances.isEmpty()) {
      this.instance = HazelcastClient.newHazelcastClient(clientConfig);
    } else {
      this.instance = instances.iterator().next();
    }
  }

  @Override
  public <T> List<T> getList(String name) {
    return instance.getList(session + name);
  }

  @Override
  public <T> void setList(String name, List<T> list) {
    IList<T> cacheList = instance.getList(session + name);
    cacheList.clear();
    cacheList.addAll(list);
  }

  @Override
  public <K, V> Map<K, V> getMap(String name) {

    return instance.getMap(session + name);
  }

  @Override
  public <K, V> void setMap(String name, Map<K, V> map) {
    IMap<K, V> cacheMap = instance.getMap(session + name);
    cacheMap.clear();
    cacheMap.putAll(map);
  }

  @Override
  public void delete(String name) {
    instance.removeDistributedObjectListener(session + name);
  }

  @Override
  public long getCounter(String name) {
    return instance.getAtomicLong(session + name).get();
  }

  @Override
  public void setCounter(String name, long count) {
    instance.getAtomicLong(session + name).set(count);
  }

  @Override
  public long incrementAndGetCounter(String name) {
    return instance.getAtomicLong(session + name).incrementAndGet();
  }

  @Override
  public void waitForCounterToReach(String name, int count) throws
    InterruptedException {
    boolean loop = true;
    while (loop) {
      if (instance.getAtomicLong(session + name).get() == count) {
        loop = false;
      }
      Thread.sleep(100);
    }
  }

  @Override
  public void waitForEvent(String name) throws InterruptedException {
    waitForCounterToReach(session + name, 1);
  }

  @Override
  public void triggerEvent(String name) throws InterruptedException {
    incrementAndGetCounter(session + name);
  }

  @Override
  public void resetCounter(String name) {
    instance.getAtomicLong(session + name).set(0L);
  }

  @Override
  public void addAndGetCounter(String name, long count) {
    instance.getAtomicLong(session + name).addAndGet(count);
  }
}
