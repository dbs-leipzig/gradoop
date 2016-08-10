package org.gradoop.common.cache.api;

public interface DistributedCacheServer extends DistributedCacheConnection {
  String getAddress();

  void addCounter(String name);
}
