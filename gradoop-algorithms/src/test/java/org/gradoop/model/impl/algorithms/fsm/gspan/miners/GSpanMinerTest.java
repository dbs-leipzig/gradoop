package org.gradoop.model.impl.algorithms.fsm.gspan.miners;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.GSpanGraphTransactionsEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.WithCount;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class GSpanMinerTest extends GradoopFlinkTestBase {

  @Test
  public void testMinersSeparatelyDirected() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(10, 1, true, getConfig())
        .execute();

    float threshold = 0.2f;

    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    DataSet<GSpanGraph> edges = encoder.encode(transactions, fsmConfig);

    for (GSpanMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        transactions.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

      Assert.assertEquals(
        PredictableTransactionsGenerator
          .containedDirectedFrequentSubgraphs(threshold),
        frequentSubgraphs.count());
    }
  }

  @Test
  public void testMinersSeparatelyUndirected() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(10, 1, true, getConfig())
        .execute();

    float threshold = 0.2f;

    FSMConfig fsmConfig = new FSMConfig(threshold, false);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    DataSet<GSpanGraph> edges = encoder.encode(transactions, fsmConfig);

    for (GSpanMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        transactions.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

      Assert.assertEquals(
        PredictableTransactionsGenerator
          .containedUndirectedFrequentSubgraphs(threshold),
        frequentSubgraphs.count());
    }
  }

  private Collection<GSpanMiner> getTransactionalFSMiners() {
    Collection<GSpanMiner> miners = Lists.newArrayList();

    miners.add(new GSpanBulkIteration());
    miners.add(new GSpanFilterRefine());
    return miners;
  }

  @Test
  public void testMinersVersus() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(30, 1, true, getConfig())
        .execute();

    float threshold = 0.4f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    DataSet<GSpanGraph> graphs = encoder.encode(transactions, fsmConfig);

    GSpanMiner iMiner = new GSpanBulkIteration();
    iMiner.setExecutionEnvironment(
      transactions.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedDFSCode>> iResult =
      iMiner.mine(graphs, encoder.getMinFrequency(), fsmConfig);

    GSpanMiner fsMiner = new GSpanFilterRefine();
    DataSet<WithCount<CompressedDFSCode>> frResult =
      fsMiner.mine(graphs, encoder.getMinFrequency(), fsmConfig);

    collectAndAssertTrue(Equals
      .cross(Count.count(iResult),
        Count.count(iResult.join(frResult).where(0).equalTo(0)))
    );
  }
}