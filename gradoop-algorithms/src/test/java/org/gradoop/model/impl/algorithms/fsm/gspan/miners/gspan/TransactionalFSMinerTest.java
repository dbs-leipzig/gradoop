package org.gradoop.model.impl.algorithms.fsm.gspan.miners.gspan;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.GraphCollectionTFSMEncoder;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class TransactionalFSMinerTest extends GradoopFlinkTestBase {

  @Test
  public void testMinersSeparately() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(100, 1, true, getConfig())
        .execute();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      GraphCollection
        .fromTransactions(transactions);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(1.0f);

    GraphCollectionTFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GraphCollectionTFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    for (TransactionalFSMiner<CompressedDFSCode> miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        collection.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

      Assert.assertEquals(702, frequentSubgraphs.count());
    }
  }

  private Collection<TransactionalFSMiner<CompressedDFSCode>> getTransactionalFSMiners() {
    Collection<TransactionalFSMiner<CompressedDFSCode>> miners = Lists.newArrayList();

    miners.add(new GSpanBulkIteration());
    miners.add(new GSpanFilterRefine());
    return miners;
  }

  @Test
  public void testMinersVersus() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(30, 1, true, getConfig())
        .execute();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      GraphCollection
        .fromTransactions(transactions);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

    GraphCollectionTFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GraphCollectionTFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    TransactionalFSMiner<CompressedDFSCode> iMiner = new GSpanBulkIteration();
    iMiner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedDFSCode>> iResult =
      iMiner.mine(edges, encoder.getMinFrequency(), fsmConfig);

    TransactionalFSMiner<CompressedDFSCode> fsMiner = new GSpanFilterRefine();
    DataSet<WithCount<CompressedDFSCode>> frResult =
      fsMiner.mine(edges, encoder.getMinFrequency(), fsmConfig);

    collectAndAssertTrue(Equals
      .cross(Count.count(iResult),
        Count.count(iResult.join(frResult).where(0).equalTo(0)))
    );
  }
}