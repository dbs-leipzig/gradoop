package org.gradoop.model.impl.algorithms.fsm.gspan.encoders;


import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.tlf.TLFDataSink;
import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.comparators.DFSCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.gspan.functions.MinDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GSpanEncoderTest extends GradoopFlinkTestBase {

  @Test
  public void testBenchmark() throws Exception {

    getExecutionEnvironment().setParallelism(1);

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(2, 1, true, getConfig())
      .execute();

    String tlfFile =  GSpanEncoderTest
      .class.getResource("/data/tlf").getFile() + "/benchmark.tlf";

    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new TLFDataSink<>(tlfFile, config);

    dataSink.write(transactions);

    getExecutionEnvironment().execute();

    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(tlfFile, config);

    DataSet<TLFGraph> graphs = dataSource.getTLFGraphs();

    FSMConfig fsmConfig = new FSMConfig(1.0f, true);

    GSpanEncoder tlfEncoder = new GSpanTLFGraphEncoder<>(fsmConfig);
    GSpanEncoder tnsEncoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);
    GSpanMiner miner = new GSpanBulkIteration();

    miner.setExecutionEnvironment(getExecutionEnvironment());

    DataSet<GSpanGraph> tlfSearchSpace = tlfEncoder.encode(graphs, fsmConfig);
    DataSet<GSpanGraph> tnsSearchSpace = tnsEncoder.encode(transactions, fsmConfig);

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
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      getDataSource();

    float threshold = 0.1f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>(fsmConfig);

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
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>(fsmConfig);

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
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);

    tEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);
    cEncoder.encode(dataSource.getGraphCollection(), fsmConfig);
    tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);

    List<String> tDictionary = tEncoder
      .getVertexLabelDictionary().collect().get(0);
    List<String> cDictionary = cEncoder
      .getVertexLabelDictionary().collect().get(0);
    List<String> tlfDictionary = (List<String>)tlfEncoder
      .getVertexLabelDictionary().collect().get(0);

    assertEqualDictionaries(tDictionary, cDictionary);
    assertEqualDictionaries(cDictionary, tlfDictionary);
  }

  @Test
  public void getEdgeLabelDictionary() throws Exception {
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      getDataSource();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>(fsmConfig);

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder(fsmConfig);



    tEncoder.encode(dataSource.getGraphTransactions(), fsmConfig);
    cEncoder.encode(dataSource.getGraphCollection(), fsmConfig);
    tlfEncoder.encode(dataSource.getTLFGraphs(), fsmConfig);

    List<String> tDictionary = tEncoder
      .getEdgeLabelDictionary().collect().get(0);
    List<String> cDictionary = cEncoder
      .getEdgeLabelDictionary().collect().get(0);
    List<String> tlfDictionary = (List<String>)tlfEncoder
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

  private TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> getDataSource() {
    String tlfFile =
      GSpanEncoderTest.class.getResource("/data/tlf/graphs.tlf").getFile();

    return new TLFDataSource<>(tlfFile, getConfig());
  }

}