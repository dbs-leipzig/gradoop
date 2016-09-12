package org.gradoop.flink.algorithms.fsm.gspan;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.functions.EncodeTLFGraphs;
import org.gradoop.flink.algorithms.fsm.gspan.functions.EncodeTransactions;
import org.gradoop.flink.algorithms.fsm.gspan.functions.MinDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.cache.GradoopFlinkCacheEnabledTestBase;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncodingTest extends GradoopFlinkCacheEnabledTestBase {

//  @Test
//  public void testPredictableBenchmark() throws Exception {
//    GraphTransactions transactions = new PredictableTransactionsGenerator(
//      2, 1, true, getConfig()).execute();
//
//    FSMConfig tlfConfig = new FSMConfig(
//      0.5f, false, cacheServer.getCacheClientConfiguration());
//
//    FSMConfig tnsConfig = new FSMConfig(
//      0.5f, false, cacheServer.getCacheClientConfiguration());
//
//    DistributedCache.getClient
//      (cacheServer.getCacheClientConfiguration(), tlfConfig.getSession())
//      .addAndGetCounter(Constants.GRAPH_COUNT, 0);
//
//    DistributedCache.getClient
//      (cacheServer.getCacheClientConfiguration(), tnsConfig.getSession())
//      .addAndGetCounter(Constants.GRAPH_COUNT, 0);
//
//
//    String tlfFile =  EncodingTest
//      .class.getResource("/data/tlf").getFile() + "/benchmark.tlf";
//
//    DataSink dataSink = new TLFDataSink(tlfFile, config);
//
//    dataSink.write(transactions);
//
//    getExecutionEnvironment().execute();
//
//    TLFDataSource dataSource = new TLFDataSource(tlfFile, config);
//
//    DataSet<TLFGraph> tlfGraphs = dataSource.getTLFGraphs();
//
//    DataSet<GSpanGraph> tlfSearchSpace = tlfGraphs
//      .mapPartition(new EncodeTLFGraphs(tlfConfig));
//
//    DataSet<GSpanGraph> tnsSearchSpace = transactions
//      .getTransactions()
//      .mapPartition(new EncodeTransactions(tnsConfig));
//
//
//    assertEquals(tlfSearchSpace.count(), tnsSearchSpace.count());
//  }
//
//  @Test
//  public void testYeastBenchmark() throws Exception {
//
//    String tlfFile =  EncodingTest
//      .class.getResource("/data/tlf").getFile() + "/yeast.tlf";
//
//    FSMConfig tlfConfig = new FSMConfig(
//      0.8f, false, cacheServer.getCacheClientConfiguration());
//
//    FSMConfig tnsConfig = new FSMConfig(
//      0.8f, false, cacheServer.getCacheClientConfiguration());
//
//    DistributedCache.getClient
//      (cacheServer.getCacheClientConfiguration(), tlfConfig.getSession())
//      .addAndGetCounter(Constants.GRAPH_COUNT, 0);
//
//    DistributedCache.getClient
//      (cacheServer.getCacheClientConfiguration(), tnsConfig.getSession())
//      .addAndGetCounter(Constants.GRAPH_COUNT, 0);
//
//
//    TLFDataSource dataSource = new TLFDataSource(tlfFile, config);
//
//    DataSet<TLFGraph> tlfGraphs = dataSource.getTLFGraphs();
//
//    DataSet<GSpanGraph> tlfSearchSpace = tlfGraphs
//      .mapPartition(new EncodeTLFGraphs(tlfConfig));
//
//    DataSet<GSpanGraph> tnsSearchSpace = dataSource
//      .getGraphTransactions()
//      .getTransactions()
//      .mapPartition(new EncodeTransactions(tnsConfig));
//
//    assertEquals(tlfSearchSpace.count(), tnsSearchSpace.count());
//  }

  @Test
  public void compareEncoding() throws Exception {
    TLFDataSource dataSource = getDataSource();

    float threshold = 0.1f;

    FSMConfig tlfConfig = new FSMConfig(
      threshold, false);

    FSMConfig tnsConfig = new FSMConfig(
      threshold, false);

    DistributedCache.getClient
      (cacheServer.getCacheClientConfiguration(), tlfConfig.getSession())
      .addAndGetCounter(Constants.GRAPH_COUNT, 0);

    DistributedCache.getClient
      (cacheServer.getCacheClientConfiguration(), tnsConfig.getSession())
      .addAndGetCounter(Constants.GRAPH_COUNT, 0);


    List<DFSCode> fsgsFromTransactions = dataSource
      .getGraphTransactions()
      .getTransactions()
      .mapPartition(new EncodeTransactions(tnsConfig))
      .map(new MinDFSCode(tnsConfig))
      .collect();

    List<DFSCode> fsgsFromTLFGraphs = dataSource
      .getTLFGraphs()
      .mapPartition(new EncodeTLFGraphs(tlfConfig))
      .map(new MinDFSCode(tlfConfig))
      .collect();

    assertTrue(
      GradoopTestUtils.equalContent(fsgsFromTransactions, fsgsFromTLFGraphs));
  }

  private TLFDataSource getDataSource() {
    String tlfFile = EncodingTest.class
      .getResource("/data/tlf/graphs.tlf").getFile();

    return new TLFDataSource(tlfFile, getConfig());
  }

}