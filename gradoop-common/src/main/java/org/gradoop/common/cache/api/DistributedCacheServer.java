package org.gradoop.common.cache.api;

public interface DistributedCacheServer {
  DistributedCacheClientConfiguration getCacheClientConfiguration();
  void shutdown();
}
