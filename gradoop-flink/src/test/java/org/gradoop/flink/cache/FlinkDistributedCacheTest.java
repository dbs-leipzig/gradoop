package org.gradoop.flink.cache;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.cache.api.DistributedCacheClientConfiguration;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlinkDistributedCacheTest extends
  GradoopFlinkCacheEnabledTestBase {

  private static final String READ_NAME = "read";
  private static final String WRITE_NAME = "write";

  /**
   * Read from distributed cache in a map function
   *
   * @throws Exception
   */
  @Test
  public void testMapRead() throws Exception {
    DistributedCacheClient client = DistributedCache.getClient(
      cacheServer.getCacheClientConfiguration(), "");

    client.getList(READ_NAME).addAll(Lists.newArrayList("A"));

    DataSet<Long> numbers = getExecutionEnvironment()
      .generateSequence(1, 4);

    DataSet<String> strings = numbers
      .map(new TestMapRead(cacheServer.getCacheClientConfiguration()));

    Collection<String> res = strings.collect();
    Collection<String> exp = Lists.newArrayList("A", "A", "A", "A");

    assertTrue(GradoopTestUtils.equalContent(exp, res));
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
      List<String> strings = client.getList(READ_NAME);
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

    DistributedCacheClient client = DistributedCache.getClient(
      cacheServer.getCacheClientConfiguration(), "");

    getExecutionEnvironment()
      .fromCollection(exp)
      .map(new TestMapWrite(cacheServer.getCacheClientConfiguration()))
      .count();

    Collection<String> res = client.getList(WRITE_NAME);
    res = Lists.newArrayList(res);

    System.out.println(exp);
    System.out.println(res);

    assertTrue(GradoopTestUtils.equalContent(exp, res));

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
      client.getList(WRITE_NAME).add(value);
      return value;
    }
  }

  @Test
  public void testPartitionIteration() throws Exception {
    Collection<Long> res = getExecutionEnvironment()
      .generateSequence(1, 100)
      .mapPartition(
        new testPartitionIteration(cacheServer.getCacheClientConfiguration()))
      .collect();

    assertEquals(1, res.size());
    assertEquals(100L, (long) res.iterator().next());
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
}