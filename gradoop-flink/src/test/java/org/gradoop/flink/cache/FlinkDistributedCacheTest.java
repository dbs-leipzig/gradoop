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
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
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
    DistributedCacheClient client = DistributedCache.getClient(
      server.getCacheClientConfiguration(), "");

    client.getList(NAME).addAll(Lists.newArrayList("A"));

    DataSet<Long> numbers = getExecutionEnvironment()
      .generateSequence(1, 4);

    DataSet<String> strings = numbers
      .map(new TestMapRead(server.getCacheClientConfiguration()));

    Collection<String> res = strings.collect();
    Collection<String> exp = Lists.newArrayList("A", "A", "A", "A");

    assertTrue(GradoopTestUtils.equalContent(exp, res));

    server.shutdown();
  }

  private class TestMapRead implements MapFunction<Long, String> {
    private final DistributedCacheClientConfiguration configuration;

    public TestMapRead(DistributedCacheClientConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public String map(Long value) throws Exception {
      DistributedCacheClient client =
        DistributedCache.getClient(configuration, "");
      List<String> strings = client.getList(NAME);
      String s = strings.get(0);
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
    DistributedCacheClient client = DistributedCache.getClient(
      server.getCacheClientConfiguration(), "");

    getExecutionEnvironment()
      .fromCollection(exp)
      .map(new TestMapWrite(server.getCacheClientConfiguration()))
      .count();

    Collection<String> res = client.getList(NAME);
    res = Lists.newArrayList(res);

    assertTrue(GradoopTestUtils.equalContent(exp, res));

    server.shutdown();
  }

  private class TestMapWrite implements MapFunction<String, String> {
    private final DistributedCacheClientConfiguration configuration;

    public TestMapWrite(DistributedCacheClientConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public String map(String value) throws Exception {
      DistributedCacheClient client = DistributedCache
        .getClient(configuration, "");
      client.getList(NAME).add(value);
      return value;
    }
  }

  @Test
  public void testPartitionIteration() throws Exception {
    DistributedCacheServer server = DistributedCache.getServer();

    Collection<Long> res = getExecutionEnvironment()
      .generateSequence(1, 100)
      .mapPartition(new testPartitionIteration(server.getCacheClientConfiguration()))
      .collect();

    assertEquals(1, res.size());
    assertEquals(100L, (long) res.iterator().next());

    server.shutdown();
  }

  private class testPartitionIteration
    extends RichMapPartitionFunction<Long, Long> {

    private final DistributedCacheClientConfiguration configuration;

    public testPartitionIteration(DistributedCacheClientConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public void mapPartition(
      Iterable<Long> values, Collector<Long> out) throws Exception {
      DistributedCacheClient client =
        DistributedCache.getClient(configuration, "");

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
//      .flatMap(new TestChainedMapWrite(server.getCacheClientConfiguration()))
//      .flatMap(new TestChainedMapRead(server.getCacheClientConfiguration()))
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
//    private final String configuration;
//    private long maxValue = 0L;
//
//    public TestChainedMapWrite(String configuration) {
//      this.configuration = configuration;
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
//      DistributedCacheClient client = DistributedCache.getClient(configuration);
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
//    private final String configuration;
//    private long maxValue = 0L;
//
//    public TestChainedMapRead(String configuration) {
//      this.configuration = configuration;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//      DistributedCacheClient client = DistributedCache.getClient(configuration);
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