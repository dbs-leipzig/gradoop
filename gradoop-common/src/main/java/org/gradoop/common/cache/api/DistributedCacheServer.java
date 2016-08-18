package org.gradoop.common.cache.api;

public interface DistributedCacheServer extends DistributedCacheConnection {
  DistributedCacheClientConfiguration getCacheClientConfiguration();

  void resetCounter(String name);
}
