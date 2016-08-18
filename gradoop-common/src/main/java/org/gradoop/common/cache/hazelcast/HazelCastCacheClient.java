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

public class HazelCastCacheClient implements DistributedCacheClient {

  private final HazelcastInstance instance;

  public HazelCastCacheClient(
    DistributedCacheClientConfiguration cacheClientConfiguration) {

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig()
      .setAddresses(
        Lists.newArrayList(cacheClientConfiguration.getServerAddress()));

    Collection<HazelcastInstance> instances =
      HazelcastClient.getAllHazelcastClients();

    if (instances.isEmpty()) {
      this.instance = HazelcastClient.newHazelcastClient(clientConfig);
    } else {
      this.instance = instances.iterator().next();
    }
  }

  @Override
  public void shutdown() {
    this.instance.shutdown();
  }


  @Override
  public <T> List<T> getList(String name) {
    return instance.getList(name);
  }

  @Override
  public <T> void setList(String name, List<T> list) {
    IList<T> cacheList = instance.getList(name);
    cacheList.clear();
    cacheList.addAll(list);
  }

  @Override
  public <K, V> Map<K, V> getMap(String name) {
    return instance.getMap(name);
  }

  @Override
  public <K, V> void setMap(String name, Map<K, V> map) {
    IMap<K, V> cacheMap = instance.getMap(name);
    cacheMap.clear();
    cacheMap.putAll(map);
  }

  @Override
  public void delete(String name) {
    instance.removeDistributedObjectListener(name);
  }

  @Override
  public long getCounter(String name) {
    return instance.getAtomicLong(name).get();
  }

  @Override
  public void setCounter(String name, long count) {
    instance.getAtomicLong(name).set(count);
  }

  @Override
  public long incrementAndGetCounter(String name) {
    return instance.getAtomicLong(name).incrementAndGet();
  }

  @Override
  public void waitForCounterToReach(String counterName, int count) throws
    InterruptedException {
    boolean loop = true;
    while (loop) {
      if (instance.getAtomicLong(counterName).get() == count) {
        loop = false;
      }
      Thread.sleep(100);
    }
  }

  @Override
  public void waitForEvent(String eventName) throws InterruptedException {
    waitForCounterToReach(eventName, 1);
  }

  @Override
  public void triggerEvent(String eventName) throws InterruptedException {
    incrementAndGetCounter(eventName);
  }

  @Override
  public void resetCounter(String name) {
    instance.getAtomicLong(name).set(0L);
  }

  @Override
  public void addAndGetCounter(String counterName, long count) {
    instance.getAtomicLong(counterName).addAndGet(count);
  }
}
