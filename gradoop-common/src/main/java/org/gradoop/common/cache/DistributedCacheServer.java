package org.gradoop.common.cache;

public interface DistributedCacheServer extends DistributedCacheConnection {
  String getAddress();
}
