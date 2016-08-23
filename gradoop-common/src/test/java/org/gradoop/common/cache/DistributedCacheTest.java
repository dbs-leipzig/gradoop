package org.gradoop.common.cache;

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheServer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributedCacheTest {

  private final DistributedCacheServer server;

  public DistributedCacheTest() {
    server = DistributedCache.getServer();
  }

  @Test
  public void testRead() throws InterruptedException {
    DistributedCacheClient client =
      DistributedCache
        .getClient(server.getCacheClientConfiguration(), "read");

    String key = "Hello";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    client.getList(key).addAll(in);
    List<String> out = client.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));
  }

  @Test
  public void testWrite() throws InterruptedException {
    DistributedCacheClient client =
      DistributedCache
        .getClient(server.getCacheClientConfiguration(), "write");

    String key = "World";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    for (String s : in) {
      client.getList(key).add(s);
    }

    List<String> out = client.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));
  }

  @Test
  public void testMultipleServers() {
    DistributedCache.getServer();
    assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
  }

  @Test
  public void testUniqueClient() throws InterruptedException {
    DistributedCache.getClient(server.getCacheClientConfiguration(), "");
    DistributedCache.getClient(server.getCacheClientConfiguration(), "");

    assertEquals(1, HazelcastClient.getAllHazelcastClients().size());
  }
}