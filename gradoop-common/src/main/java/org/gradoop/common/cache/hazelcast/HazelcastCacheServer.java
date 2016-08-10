package org.gradoop.common.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.gradoop.common.cache.api.DistributedCacheServer;

import java.util.List;
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
}
