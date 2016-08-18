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
    DistributedCacheServer server =
      DistributedCache.getServer();
    DistributedCacheClient client =
      DistributedCache.getClient(server.getCacheClientConfiguration());

    String key = "Hello";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    server.getList(key).addAll(in);
    List<String> out = client.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));

    client.shutdown();
    server.shutdown();
  }

  @Test
  public void testWrite() {
    DistributedCacheServer server =
      DistributedCache.getServer();
    DistributedCacheClient client =
      DistributedCache.getClient(server.getCacheClientConfiguration());

    String key = "Hello";
    List<String> in = Lists.newArrayList("Distributed", "Cache");

    for (String s : in) {
      client.getList(key).add(s);
    }
    List<String> out = server.getList(key);

    assertTrue(GradoopTestUtils.equalContent(in, out));

    client.shutdown();
    server.shutdown();
  }

  @Test
  public void testMultipleServers() {
    DistributedCacheServer server1 = DistributedCache.getServer();
    DistributedCacheServer server2 = DistributedCache.getServer();

    assertNotNull(Hazelcast.getHazelcastInstanceByName(
      server1.getCacheClientConfiguration().getCacheName()));

    assertNotNull(Hazelcast.getHazelcastInstanceByName(
      server2.getCacheClientConfiguration().getCacheName()));

    assertEquals(2, Hazelcast.getAllHazelcastInstances().size());

    server1.shutdown();
    server2.shutdown();
  }

  @Test
  public void testUniqueClient() {
    Hazelcast.shutdownAll();

    DistributedCacheServer server = DistributedCache.getServer();

    DistributedCacheClient client1 =
      DistributedCache.getClient(server.getCacheClientConfiguration());
    DistributedCacheClient client2 =
      DistributedCache.getClient(server.getCacheClientConfiguration());

    assertEquals(1, HazelcastClient.getAllHazelcastClients().size());

    client1.shutdown();
    client2.shutdown();
    server.shutdown();
  }
}