package org.gradoop.common.cache.api;

import java.util.List;
import java.util.Map;

/**
 * Created by peet on 10.08.16.
 */
public interface DistributedCacheConnection {
  void shutdown();

  <T> List<T> getList(String name);

  <T> void setList(String name, List<T> list);

  <K, V> Map<K, V> getMap(String name);

  <K, V> void setMap(String name, Map<K, V> map);

  void delete(String name);
}
