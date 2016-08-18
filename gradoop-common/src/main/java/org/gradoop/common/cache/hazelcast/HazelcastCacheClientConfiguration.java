package org.gradoop.common.cache.hazelcast;

import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;

/**
 * Created by peet on 18.08.16.
 */
public class HazelcastCacheClientConfiguration
  implements DistributedCacheClientConfiguration {

  private final String serverAddress;

  public HazelcastCacheClientConfiguration(String serverAddress) {
    this.serverAddress = serverAddress;
  }

  @Override
  public String getServerAddress() {
    return serverAddress;
  }


  @Override
  public String toString() {
    return "@" + serverAddress;
  }
}
