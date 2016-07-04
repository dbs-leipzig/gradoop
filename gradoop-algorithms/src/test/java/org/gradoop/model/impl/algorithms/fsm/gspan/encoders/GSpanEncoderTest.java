package org.gradoop.model.impl.algorithms.fsm.gspan.encoders;


import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.comparators.DFSCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.gspan.functions.MinDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GSpanEncoderTest extends GradoopFlinkTestBase {

  @Test
  public void encode() throws Exception {
    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      getDataSource();

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>();

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>();

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.1f);

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

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>();

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>();

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

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

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>();

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>();

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

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

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      tEncoder = new GSpanGraphTransactionsEncoder<>();

    GSpanGraphCollectionEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      cEncoder = new GSpanGraphCollectionEncoder<>();

    GSpanTLFGraphEncoder tlfEncoder = new GSpanTLFGraphEncoder();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

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