package org.gradoop.flink.cache;

import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheServer;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class GradoopFlinkCacheEnabledTestBase
  extends GradoopFlinkTestBase {

  protected static DistributedCacheServer cacheServer;

  @BeforeClass
  public static void startCache() {
    cacheServer = DistributedCache.getServer();
  }

  @AfterClass
  public static void shutDownCache() throws InterruptedException {
   cacheServer.shutdown();
   Thread.sleep(3000);
  }
}
