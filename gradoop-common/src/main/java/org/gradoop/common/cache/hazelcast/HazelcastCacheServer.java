package org.gradoop.common.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
import org.gradoop.common.cache.api.DistributedCacheServer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class HazelcastCacheServer implements DistributedCacheServer {

  private final DistributedCacheClientConfiguration cacheClientConfiguration;
  private final HazelcastInstance instance;

  public HazelcastCacheServer(String serverAddress) {
    this.cacheClientConfiguration = new HazelcastCacheClientConfiguration
      (serverAddress, UUID.randomUUID().toString());

    Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();

    if (instances.isEmpty()) {
      this.instance = Hazelcast.newHazelcastInstance();
    } else {
      this.instance = instances.iterator().next();
    }
  }

  @Override
  public DistributedCacheClientConfiguration getCacheClientConfiguration() {
    return cacheClientConfiguration;
  }

  @Override
  public void resetCounter(String name) {
    instance.getAtomicLong(name).set(0L);
  }

  @Override
  public void shutdown() {
    instance.shutdown();
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
}
