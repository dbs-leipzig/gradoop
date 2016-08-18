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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DistributedCacheTest {

  @Test
  public void testRead() {
    Hazelcast.shutdownAll();

    DistributedCacheServer server =
      DistributedCache.getServer();
    DistributedCacheClient client =
      DistributedCache.getClient(server.getCacheClientConfiguration(), "");

    String key = "Hello";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    client.getList(key).addAll(in);
    List<String> out = client.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));

    server.shutdown();
  }

  @Test
  public void testWrite() {
    DistributedCacheServer server =
      DistributedCache.getServer();
    DistributedCacheClient client =
      DistributedCache.getClient(server.getCacheClientConfiguration(), "");

    String key = "Hello";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    for (String s : in) {
      client.getList(key).add(s);
    }

    List<String> out = client.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));

    server.shutdown();
  }

  @Test
  public void testMultipleServers() {
    DistributedCacheServer server1 = DistributedCache.getServer();
    DistributedCacheServer server2 = DistributedCache.getServer();

    assertEquals(1, Hazelcast.getAllHazelcastInstances().size());

    server1.shutdown();
  }

  @Test
  public void testUniqueClient() {
    DistributedCacheServer server = DistributedCache.getServer();

    DistributedCache.getClient(server.getCacheClientConfiguration(), "");
    DistributedCache.getClient(server.getCacheClientConfiguration(), "");

    assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

    server.shutdown();
  }
}