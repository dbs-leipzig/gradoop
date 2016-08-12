package org.gradoop.common.cache.api;

public interface DistributedCacheClient extends DistributedCacheConnection {
  long incrementAndGetCounter(String name);

  void waitForCounterToReach(String counterName, int count) throws
    InterruptedException;

  void waitForEvent(String eventName) throws InterruptedException;

  void triggerEvent(String eventName) throws InterruptedException;
}
