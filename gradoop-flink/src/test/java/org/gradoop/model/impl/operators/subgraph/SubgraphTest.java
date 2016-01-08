package org.gradoop.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;

public class SubgraphTest extends GradoopFlinkTestBase {

  /**
   * Extracts a subgraph that exists in the graph and is valid.
   *
   * @throws IOException
   */
  @Test
  public void testExistingSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave);" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave);" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob);" +
      "(frank)-[fkc]->(carol);" +
      "(frank)-[fkd]->(dave);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Person");
      }
    };

    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>() {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        return edgePojo.getLabel().equals("knows");
      }
    };

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      input.subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice);(bob);(carol);(dave);(eve);(frank);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Person");
      }
    };

    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>() {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        return edgePojo.getLabel().equals("friendOf");
      }
    };

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      input.subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph which is empty.
   *
   * @throws Exception
   */
  @Test
  public void testEmptySubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("User");
      }
    };

    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>() {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        return edgePojo.getLabel().equals("friendOf");
      }
    };

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      input.subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs);" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Forum")
          || vertexPojo.getLabel().equals("Tag") ;
      }
    };

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      input.vertexInducedSubgraph(vertexFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs);" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop);" +
      "]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>() {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        return edgePojo.getLabel().equals("hasTag");
      }
    };

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output =
      input.edgeInducedSubgraph(edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }
}
