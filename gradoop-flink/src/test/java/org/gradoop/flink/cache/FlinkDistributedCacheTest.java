package org.gradoop.flink.cache;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheServer;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import javax.print.attribute.standard.MediaSize;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlinkDistributedCacheTest extends GradoopFlinkTestBase {

  private static final String NAME = "name";

  /**
   * Read from distributed cache in a map function
   *
   * @throws Exception
   */
  @Test
  public void testMapRead() throws Exception {
    DistributedCacheServer server = DistributedCache.getServer();

    server.getList(NAME).addAll(Lists.newArrayList("A"));

    DataSet<Long> numbers = getExecutionEnvironment()
      .generateSequence(1, 4);

    DataSet<String> strings = numbers
      .map(new TestMapRead(server.getAddress()));

    Collection<String> res = strings.collect();
    Collection<String> exp = Lists.newArrayList("A", "A", "A", "A");

    assertTrue(GradoopTestUtils.equalContent(exp, res));

    server.shutdown();
  }

  private class TestMapRead implements MapFunction<Long, String> {
    private final String serverAddress;

    public TestMapRead(String serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    public String map(Long value) throws Exception {
      DistributedCacheClient client = DistributedCache.getClient(serverAddress);
      List<String> strings = client.getList(NAME);
      String s = strings.get(0);
      client.shutdown();
      return s;
    }
  }

  /**
   * Write to distributed cache in a map function
   *
   * @throws Exception
   */
  @Test
  public void testMapWrite() throws Exception {
    Collection<String> exp = Lists.newArrayList("A", "B", "C", "D");

    DistributedCacheServer server = DistributedCache.getServer();

    long count = getExecutionEnvironment()
      .fromCollection(exp)
      .map(new TestMapWrite(server.getAddress()))
      .count();

    System.out.println(count);

    Collection<String> res = server.getList(NAME);
    res = Lists.newArrayList(res);

    System.out.println(res);

    assertTrue(GradoopTestUtils.equalContent(exp, res));

    server.shutdown();
  }

  private class TestMapWrite implements MapFunction<String, String> {
    private final String serverAddress;

    public TestMapWrite(String serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    public String map(String value) throws Exception {
      DistributedCacheClient client = DistributedCache.getClient(serverAddress);
      client.getList(NAME).add(value);
      client.shutdown();
      return value;
    }
  }
}