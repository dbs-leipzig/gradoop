package org.gradoop.common.cache.hazelcast;

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.gradoop.common.cache.api.DistributedCacheClient;

import java.util.Collection;
import java.util.List;

public class HazelCastCacheClient implements DistributedCacheClient {

  private final HazelcastInstance instance;

  public HazelCastCacheClient(String serverAddress) {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig()
      .setAddresses(Lists.newArrayList(serverAddress));

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
}
