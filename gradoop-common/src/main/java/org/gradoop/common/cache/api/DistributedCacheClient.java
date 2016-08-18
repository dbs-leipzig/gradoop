package org.gradoop.common.cache.api;

import java.util.List;
import java.util.Map;

public interface DistributedCacheClient extends DistributedCacheConnection {
  long incrementAndGetCounter(String name);

  void waitForCounterToReach(String counterName, int count) throws
    InterruptedException;

  void waitForEvent(String eventName) throws InterruptedException;

  void triggerEvent(String eventName) throws InterruptedException;

  void resetCounter(String name);

  void addAndGetCounter(String counterName, long graphCount);

  <T> List<T> getList(String name);

  <T> void setList(String name, List<T> list);

  <K, V> Map<K, V> getMap(String name);

  <K, V> void setMap(String name, Map<K, V> map);

  void delete(String name);

  long getCounter(String name);

  void setCounter(String name, long count);
}
