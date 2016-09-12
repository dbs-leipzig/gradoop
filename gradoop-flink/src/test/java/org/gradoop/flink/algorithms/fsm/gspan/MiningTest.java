package org.gradoop.flink.algorithms.fsm.gspan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.functions.EncodeTransactions;
import org.gradoop.flink.algorithms.fsm.gspan.functions.IterativeGSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.cache.GradoopFlinkCacheEnabledTestBase;
import org.gradoop.flink.datagen.transactions.predictable
  .PredictableTransactionsGenerator;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Assert;
import org.junit.Test;

public class MiningTest extends GradoopFlinkCacheEnabledTestBase {

  @Test
  public void testMinersSeparatelyDirected() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      10, 1, true, getConfig()).execute();

    float threshold = 0.2f;

    FSMConfig fsmConfig = new FSMConfig(
      threshold, true);

    DistributedCache.getClient
      (cacheServer.getCacheClientConfiguration(), fsmConfig.getSession())
      .addAndGetCounter(Constants.GRAPH_COUNT, 0);

    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs = transactions
      .getTransactions()
      .mapPartition(new EncodeTransactions(fsmConfig))
      .mapPartition(new IterativeGSpan(fsmConfig));

    Assert.assertEquals(
      PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(threshold),
      frequentSubgraphs.count()
    );
  }

  @Test
  public void testMinersSeparatelyUndirected() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      10, 1, true, getConfig()).execute();

    float threshold = 0.2f;

    FSMConfig fsmConfig = new FSMConfig(
      threshold, false);

    DistributedCache.getClient
      (cacheServer.getCacheClientConfiguration(), fsmConfig.getSession())
      .addAndGetCounter(Constants.GRAPH_COUNT, 0);

    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
      transactions
      .getTransactions()
      .mapPartition(new EncodeTransactions(fsmConfig))
      .mapPartition(new IterativeGSpan(fsmConfig));


    Assert.assertEquals(
      PredictableTransactionsGenerator
        .containedUndirectedFrequentSubgraphs(threshold),
      frequentSubgraphs.count()
    );
  }

//  @Test
//  public void testPredictableBenchmark() throws Exception {
//    GraphTransactions transactions = new PredictableTransactionsGenerator(
//      10, 1, true, getConfig()).execute();
//
//    FSMConfig tlfConfig = new FSMConfig(
//      0.5f, true, cacheServer.getCacheClientConfiguration());
//
//    FSMConfig tnsConfig = new FSMConfig(
//      0.5f, true, cacheServer.getCacheClientConfiguration());
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
////    assertEquals(tlfSearchSpace.count(), tnsSearchSpace.count());
//
//    // mine
//    DataSet<WithCount<CompressedDFSCode>> tlfFrequentSubgraphs = tlfSearchSpace
//      .mapPartition(new IterativeGSpan(tlfConfig));
//
//    DataSet<WithCount<CompressedDFSCode>> tnsFrequentSubgraphs = tnsSearchSpace
//      .mapPartition(new IterativeGSpan(tnsConfig));
//
//    assertEquals(tlfFrequentSubgraphs.count(), tnsFrequentSubgraphs.count());
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
//    // mine
//    DataSet<WithCount<CompressedDFSCode>> tlfFrequentSubgraphs = tlfSearchSpace
//      .mapPartition(new IterativeGSpan(tlfConfig));
//
//    DataSet<WithCount<CompressedDFSCode>> tnsFrequentSubgraphs = tnsSearchSpace
//      .mapPartition(new IterativeGSpan(tnsConfig));
//
//    assertEquals(tlfFrequentSubgraphs.count(), tnsFrequentSubgraphs.count());
//  }

}