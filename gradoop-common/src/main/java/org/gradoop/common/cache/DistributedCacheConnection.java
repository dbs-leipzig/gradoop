package org.gradoop.common.cache;

import java.util.List;

/**
 * Created by peet on 10.08.16.
 */
public interface DistributedCacheConnection {
  void shutdown();

  <T> List<T> getList(String name);
}
