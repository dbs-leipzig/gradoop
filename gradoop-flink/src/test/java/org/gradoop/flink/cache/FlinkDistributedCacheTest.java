package org.gradoop.flink.cache;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheServer;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

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

    getExecutionEnvironment()
      .fromCollection(exp)
      .map(new TestMapWrite(server.getAddress()))
      .count();

    Collection<String> res = server.getList(NAME);
    res = Lists.newArrayList(res);

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

  /**
   * Write to distributed cache in a map close function
   *
   * @throws Exception
   */
  @Test
  public void testMapCloseWrite() throws Exception {
    Collection<String> exp = Lists.newArrayList(
      "A", "B", "C", "D", "E", "F", "G", "H");

    DistributedCacheServer server = DistributedCache.getServer();

    getExecutionEnvironment()
      .fromCollection(exp)
      .map(new TestMapCloseWrite(server.getAddress()))
      .count();

    Collection<String> res = server.getList(NAME);
    res = Lists.newArrayList(res);

    assertTrue(GradoopTestUtils.equalContent(exp, res));

    server.shutdown();
  }

  private class TestMapCloseWrite extends RichMapFunction<String, String> {
    private final String serverAddress;
    private Collection<String> values = Lists.newArrayList();

    public TestMapCloseWrite(String serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    public String map(String value) throws Exception {
      values.add(value);
      return value;
    }

    @Override
    public void close() throws Exception {
      DistributedCacheClient client = DistributedCache.getClient(serverAddress);
      client.getList(NAME).addAll(values);
      client.shutdown();
      super.close();
    }
  }

  @Test
  public void testPartitionIteration() throws Exception {
    DistributedCacheServer server = DistributedCache.getServer();

    server.addCounter("finishedPartions");

    Collection<Long> res = getExecutionEnvironment()
      .generateSequence(1, 100)
      .mapPartition(new testPartitionIteration(server.getAddress()))
      .collect();

    assertEquals(1, res.size());
    assertEquals(100L, (long) res.iterator().next());

    server.shutdown();
  }

  private class testPartitionIteration
    extends RichMapPartitionFunction<Long, Long> {

    private final String serverAddress;

    public testPartitionIteration(String serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    public void mapPartition(
      Iterable<Long> values, Collector<Long> out) throws Exception {
      DistributedCacheClient client = DistributedCache.getClient(serverAddress);

      Long localMaxValue = 0L;
      long partitions = getRuntimeContext().getNumberOfParallelSubtasks();

      for (Long value : values) {
        if (value > localMaxValue) {
          localMaxValue = value;
        }
      }

      List<Long> maxValues = client.getList("maxValues");
      maxValues.add(localMaxValue);

      long finishedPartitions = client
        .incrementAndGetCounter("finishedPartions");

      if (partitions == finishedPartitions) {
        for (long maxValue : maxValues) {
          if (maxValue > localMaxValue) {
            localMaxValue = maxValue;
          }
        }

        out.collect(localMaxValue);
      }

      client.shutdown();
    }
  }

//  /**
//   * Write to distributed cache in a map close function
//   *
//   * @throws Exception
//   */
//  @Test
//  public void testChainedMapWriteRead() throws Exception {
//    Collection<String> exp = Lists.newArrayList(
//      "A", "B", "C", "D", "E", "F", "G", "H");
//
//    DistributedCacheServer server = DistributedCache.getServer();
//
//    Collection<Long> res = getExecutionEnvironment()
//      .generateSequence(1, 10)
//      .flatMap(new TestChainedMapWrite(server.getAddress()))
//      .flatMap(new TestChainedMapRead(server.getAddress()))
//      .collect();
//
//    assertEquals(10, res.size());
//
//    for (long i : res) {
//      assertEquals(10L, i);
//    }
//
//    server.shutdown();
//  }
//
//  private class TestChainedMapWrite extends RichFlatMapFunction<Long, Long> {
//
//    private final String serverAddress;
//    private long maxValue = 0L;
//
//    public TestChainedMapWrite(String serverAddress) {
//      this.serverAddress = serverAddress;
//    }
//
//    @Override
//    public void flatMap(Long value, Collector<Long> out) throws Exception {
//
//      if (value > maxValue) {
//        maxValue = value;
//      }
//
//      System.out.println("map 1");
//
//      out.collect(maxValue);
//    }
//
//    @Override
//    public void close() throws Exception {
//      DistributedCacheClient client = DistributedCache.getClient(serverAddress);
//      client.getList(NAME).add(maxValue);
//      client.shutdown();
//
//      System.out.println("close 1");
//
//      super.close();
//    }
//
//  }
//
//  private class TestChainedMapRead extends RichFlatMapFunction<Long, Long> {
//
//    private final String serverAddress;
//    private long maxValue = 0L;
//
//    public TestChainedMapRead(String serverAddress) {
//      this.serverAddress = serverAddress;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//      DistributedCacheClient client = DistributedCache.getClient(serverAddress);
//      List<Long> maxValues = client.getList(NAME);
//
//      System.out.println("open 2");
//
//      for (Long value : maxValues) {
//        if (value > maxValue) {
//          maxValue = value;
//        }
//      }
//
//      client.shutdown();
//      super.open(parameters);
//    }
//
//    @Override
//    public void flatMap(Long value, Collector<Long> out) throws Exception {
//
//      System.out.println("map 2");
//
//      out.collect(maxValue);
//    }
//  }
}