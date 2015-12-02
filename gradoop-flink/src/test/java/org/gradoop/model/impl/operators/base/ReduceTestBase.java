package org.gradoop.model.impl.operators.base;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;

import static org.junit.Assert.assertTrue;

public abstract class ReduceTestBase extends GradoopFlinkTestBase {

  protected void checkExpectationsEqualResults(
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader,
    UnaryCollectionToGraphOperator<GraphHeadPojo, VertexPojo, EdgePojo> operator
  ) throws Exception {
    // overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col13 =
      loader.getGraphCollectionByVariables("g1", "g3");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp13 =
      loader.getLogicalGraphByVariable("exp13");

    // no overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp12 =
      loader.getLogicalGraphByVariable("exp12");

    // full overlap
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col14 =
      loader.getGraphCollectionByVariables("g1", "g4");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> exp14 =
      loader.getLogicalGraphByVariable("exp14");

    assertTrue("partial overlap failed",
      operator.execute(col13).equalsByElementData(exp13).collect().get(0));
    assertTrue("without overlap failed",
      operator.execute(col12).equalsByElementData(exp12).collect().get(0));
    assertTrue("with full overlap failed",
      operator.execute(col14).equalsByElementData(exp14).collect().get(0));
  }
}
