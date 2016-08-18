package org.gradoop.common.cache.api;

import java.io.Serializable;

public interface DistributedCacheClientConfiguration extends Serializable {
  String getServerAddress();
}
