package org.gradoop.common.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.gradoop.common.cache.api.DistributedCacheServer;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HazelcastCacheServer implements DistributedCacheServer {

  private final String address;
  private final HazelcastInstance instance;

  public HazelcastCacheServer(String address) {
    this.address = address;

    Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();

    if (instances.isEmpty()) {
      this.instance = Hazelcast.newHazelcastInstance();
    } else {
      this.instance = instances.iterator().next();
    }
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public void addCounter(String name) {
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
}
