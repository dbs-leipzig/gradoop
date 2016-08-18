package org.gradoop.common.cache.hazelcast;

import com.hazelcast.config.Config;
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
  private final String prefix;

  public HazelcastCacheServer(String serverAddress) {
    prefix = UUID.randomUUID().toString();
    this.cacheClientConfiguration =
      new HazelcastCacheClientConfiguration(serverAddress, prefix);

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
  public void resetCounter(String name) {
    instance.getAtomicLong(prefix+name).set(0L);
  }

  @Override
  public void shutdown() {
    instance.getCluster().shutdown();
  }

  @Override
  public <T> List<T> getList(String name) {
    return instance.getList(prefix+name);
  }

  @Override
  public <T> void setList(String name, List<T> list) {
    IList<T> cacheList = instance.getList(prefix+name);
    cacheList.clear();
    cacheList.addAll(list);
  }

  @Override
  public <K, V> Map<K, V> getMap(String name) {
    return instance.getMap(prefix+name);
  }

  @Override
  public <K, V> void setMap(String name, Map<K, V> map) {
    IMap<K, V> cacheMap = instance.getMap(prefix+name);
    cacheMap.clear();
    cacheMap.putAll(map);
  }

  @Override
  public void delete(String name) {
    instance.removeDistributedObjectListener(prefix+name);
  }

  @Override
  public long getCounter(String name) {
    return instance.getAtomicLong(prefix+name).get();
  }

  @Override
  public void setCounter(String name, long count) {
    instance.getAtomicLong(prefix+name).set(count);
  }
}
