package org.gradoop.flink.algorithms.fsm.gspan.encoders;


import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.flink.algorithms.fsm.gspan.comparators.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.gspan.functions.MinDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GSpanEncoderTest extends GradoopFlinkTestBase {

  @Test
  public void testPredictableBenchmark() throws Exception {
    GraphTransactions transactions = new PredictableTransactionsGenerator(
      2, 1, true, getConfig()).execute();

    FSMConfig fsmConfig = new FSMConfig(1.0f, true);

    String tlfFile =  GSpanEncoderTest
      .class.getResource("/data/tlf").getFile() + "/benchmark.tlf";

    DataSink dataSink = new TLFDataSink(tlfFile, config);

    dataSink.write(transactions);

    getExecutionEnvironment().execute();

    TLFDataSource dataSource = new TLFDataSource(tlfFile, config);

    DataSet<TLFGraph> graphs = dataSource.getTLFGraphs();

    GSpanEncoder<DataSet<TLFGraph>> tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);
    GSpanEncoder<GraphTransactions> tnsEncoder = new GSpanGraphTransactionsEncoder(fsmConfig);
    GSpanMiner miner = new GSpanBulkIteration();

    miner.setExecutionEnvironment(getExecutionEnvironment());


    DataSet<GSpanGraph> tlfSearchSpace =
      tlfEncoder.encode(graphs, fsmConfig);
    DataSet<GSpanGraph> tnsSearchSpace =
      tnsEncoder.encode(transactions, fsmConfig);

    assertEquals(tlfSearchSpace.count(), tnsSearchSpace.count());

    // mine
    DataSet<WithCount<CompressedDFSCode>> tlfFrequentSubgraphs =
      miner.mine(tlfSearchSpace, tlfEncoder.getMinFrequency(), fsmConfig);
    DataSet<WithCount<CompressedDFSCode>> tnsFrequentSubgraphs =
      miner.mine(tnsSearchSpace, tlfEncoder.getMinFrequency(), fsmConfig);


    assertEquals(tlfFrequentSubgraphs.count(), tnsFrequentSubgraphs.count());

  }

  @Test
  public void testYeastBenchmark() throws Exception {

    String tlfFile =  GSpanEncoderTest
      .class.getResource("/data/tlf").getFile() + "/yeast.tlf";

    FSMConfig fsmConfig = new FSMConfig(0.5f, false);

    TLFDataSource dataSource = new TLFDataSource(tlfFile, config);

    GSpanEncoder<DataSet<TLFGraph>> tlfEncoder =
      new GSpanTLFGraphEncoder(fsmConfig);
    GSpanEncoder<GraphTransactions> tnsEncoder =
      new GSpanGraphTransactionsEncoder(fsmConfig);
    GSpanMiner miner = new GSpanBulkIteration();

    miner.setExecutionEnvironment(getExecutionEnvironment());

    DataSet<GSpanGraph> tlfSearchSpace =
      tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);
    DataSet<GSpanGraph> tnsSearchSpace =
      tnsEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);

    assertEquals(tlfSearchSpace.count(), tnsSearchSpace.count());

    // mine
    DataSet<WithCount<CompressedDFSCode>> tlfFrequentSubgraphs =
      miner.mine(tlfSearchSpace, tlfEncoder.getMinFrequency(), fsmConfig);
    DataSet<WithCount<CompressedDFSCode>> tnsFrequentSubgraphs =
      miner.mine(tnsSearchSpace, tlfEncoder.getMinFrequency(), fsmConfig);

    assertEquals(tlfFrequentSubgraphs.count(), tnsFrequentSubgraphs.count());
  }

  @Test
  public void encode() throws Exception {
    TLFDataSource dataSource = getDataSource();

    float threshold = 0.1f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder tEncoder =
      new GSpanGraphTransactionsEncoder(fsmConfig);

    GSpanGraphCollectionEncoder cEncoder =
      new GSpanGraphCollectionEncoder(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);

    List<DFSCode> tGraphs = tEncoder
      .encode(dataSource.getGraphTransactions(), fsmConfig)
      .map(new MinDFSCode(fsmConfig))
      .collect();

    List<DFSCode> cGraphs = cEncoder
      .encode(dataSource.getGraphCollection(), fsmConfig)
      .map(new MinDFSCode(fsmConfig))
      .collect();

    List<DFSCode> tlfGraphs = tlfEncoder
      .encode(dataSource.getTLFGraphs(), fsmConfig)
      .map(new MinDFSCode(fsmConfig))
      .collect();

    DFSCodeComparator comparator =
      new DFSCodeComparator(fsmConfig.isDirected());

    Collections.sort(tGraphs, comparator);
    Collections.sort(cGraphs, comparator);
    Collections.sort(tlfGraphs, comparator);

    Iterator<DFSCode> tIterator = tGraphs.iterator();
    Iterator<DFSCode> cIterator = cGraphs.iterator();
    Iterator<DFSCode> cIterator2 = cGraphs.iterator();
    Iterator<DFSCode> tlfIterator = cGraphs.iterator();

    while (tIterator.hasNext()) {
      assertEquals(tIterator.next(), cIterator.next());
      assertEquals(cIterator2.next(), tlfIterator.next());
    }
  }

  @Test
  public void getMinFrequency() throws Exception {
    TLFDataSource dataSource = getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder tEncoder =
      new GSpanGraphTransactionsEncoder(fsmConfig);

    GSpanGraphCollectionEncoder cEncoder =
      new GSpanGraphCollectionEncoder(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);

    tEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);
    cEncoder.encode(dataSource.getGraphCollection(), fsmConfig);
    tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);

    assertEquals(
      tEncoder.getMinFrequency().collect().get(0),
      cEncoder.getMinFrequency().collect().get(0)
    );
    assertEquals(
      cEncoder.getMinFrequency().collect().get(0),
      tlfEncoder.getMinFrequency().collect().get(0)
    );
  }

  @Test
  public void getVertexLabelDictionary() throws Exception {
    TLFDataSource dataSource = getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder tEncoder =
      new GSpanGraphTransactionsEncoder(fsmConfig);

    GSpanGraphCollectionEncoder cEncoder =
      new GSpanGraphCollectionEncoder(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);

    tEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);
    cEncoder.encode(dataSource.getGraphCollection(), fsmConfig);
    tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);

    List<String> tDictionary = tEncoder
      .getVertexLabelDictionary().collect().get(0);
    List<String> cDictionary = cEncoder
      .getVertexLabelDictionary().collect().get(0);
    List<String> tlfDictionary = tlfEncoder
      .getVertexLabelDictionary().collect().get(0);

    assertEqualDictionaries(tDictionary, cDictionary);
    assertEqualDictionaries(cDictionary, tlfDictionary);
  }

  @Test
  public void getEdgeLabelDictionary() throws Exception {
    TLFDataSource dataSource = getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder tEncoder =
      new GSpanGraphTransactionsEncoder(fsmConfig);

    GSpanGraphCollectionEncoder cEncoder =
      new GSpanGraphCollectionEncoder(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);

    tEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);
    cEncoder.encode(dataSource.getGraphCollection(), fsmConfig);
    tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);

    List<String> tDictionary = tEncoder
      .getEdgeLabelDictionary().collect().get(0);
    List<String> cDictionary = cEncoder
      .getEdgeLabelDictionary().collect().get(0);
    List<String> tlfDictionary = tlfEncoder
      .getEdgeLabelDictionary().collect().get(0);

    assertEqualDictionaries(tDictionary, cDictionary);
    assertEqualDictionaries(cDictionary, tlfDictionary);
  }

  private void assertEqualDictionaries(List<String> tDictionary,
    List<String> cDictionary) {
    assertEquals(tDictionary.size(), cDictionary.size());

    Iterator<String> tIterator = tDictionary.iterator();
    Iterator<String> cIterator = cDictionary.iterator();

    while (tIterator.hasNext()) {
      assertEquals(tIterator.next(), cIterator.next());
    }
  }

  private TLFDataSource getDataSource() {
    String tlfFile = GSpanEncoderTest.class
      .getResource("/data/tlf/graphs.tlf").getFile();

    return new TLFDataSource(tlfFile, getConfig());
  }

}