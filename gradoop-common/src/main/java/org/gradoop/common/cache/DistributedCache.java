package org.gradoop.common.cache;

import org.gradoop.common.util.NetworkHelper;

public class DistributedCache {

  public static DistributedCacheServer getServer() {
    return new HazelcastCacheServer(NetworkHelper.getLocalHost());
  }

  public static DistributedCacheClient getClient(String serverAddress) {
    return new HazelCastCacheClient(serverAddress);
  }
}
