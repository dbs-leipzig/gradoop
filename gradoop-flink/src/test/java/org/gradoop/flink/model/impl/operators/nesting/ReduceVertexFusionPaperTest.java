package org.gradoop.flink.model.impl.operators.nesting;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.nest.NestingWithDisjunctive;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class ReduceVertexFusionPaperTest extends GradoopFlinkTestBase {

  /**
   * Defining the hashing functions required to break down the join function
   */
  protected FlinkAsciiGraphLoader getBibNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream("/data/gdl/jointest.gdl");
    return getLoaderFromStream(inputStream);
  }

  protected void testGraphGraphGraphCollection(LogicalGraph right,
    GraphCollection gcl, LogicalGraph expected) throws Exception {
    NestingWithDisjunctive f = new NestingWithDisjunctive(GradoopId.get());
    LogicalGraph output = f.execute(right, gcl);
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * joining empties shall not return errors.
   * The two union graphs are returned
   *
   * @throws Exception
   */
  @Test
  public void with_no_graph_collection() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GraphCollection empty = loader.getGraphCollectionByVariables();
    GraphCollection data = loader.getGraphCollectionByVariables("research","citation");
    LogicalGraph expected = new ReduceCombination().execute(data);
    testGraphGraphGraphCollection(expected,empty,expected);
  }

  /**
   * fusing elements together
   * @throws Exception
   */
  @Test
  public void full_disjunctive_example() throws Exception {
    FlinkAsciiGraphLoader loader = getBibNetworkLoader();
    GraphCollection hypervertices = loader.getGraphCollectionByVariables("g0","g1","g2","g3","g4");
    GraphCollection data = loader.getGraphCollectionByVariables("research","citation");
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    LogicalGraph actual = new ReduceCombination().execute(data);
    testGraphGraphGraphCollection(actual,hypervertices,expected);
  }

}
