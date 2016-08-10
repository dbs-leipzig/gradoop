package org.gradoop.common.cache.api;

public interface DistributedCacheClient extends DistributedCacheConnection {
  long incrementAndGetCounter(String name);
}
