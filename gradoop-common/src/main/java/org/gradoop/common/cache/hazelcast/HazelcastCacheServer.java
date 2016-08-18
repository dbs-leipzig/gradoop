package org.gradoop.common.cache.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
import org.gradoop.common.cache.api.DistributedCacheServer;

import java.util.Set;

public class HazelcastCacheServer implements DistributedCacheServer {

  private final DistributedCacheClientConfiguration cacheClientConfiguration;
  private final HazelcastInstance instance;

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
