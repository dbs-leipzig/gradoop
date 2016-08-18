package org.gradoop.common.cache.hazelcast;

import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;

/**
 * Created by peet on 18.08.16.
 */
public class HazelcastCacheClientConfiguration implements
  DistributedCacheClientConfiguration {
  private final String serverAddress;
  private final String cacheName;

  public HazelcastCacheClientConfiguration(
    String serverAddress, String cacheName) {
    this.serverAddress = serverAddress;
    this.cacheName = cacheName;
  }

  @Override
  public String getServerAddress() {
    return serverAddress;
  }

  @Override
  public String getCacheName() {
    return cacheName;
  }

  @Override
  public String toString() {
    return cacheName + "@" + serverAddress;
  }
}
