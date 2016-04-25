package org.gradoop.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

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

  @Test
  public void testCollectionSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "(jay:Person {" +
        "name = \"Jay\", age=45, gender = \"f\", city = \"Leipzig\"})" +
      "g4:Community[" +
        "(jay)-[jkb:knows {since = 2016}]->(bob);" +
        "(bob)-[blj:likes]->(jay);" +
        "]");

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(alice);" +
        "(bob);" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[]"
    );

    loader.appendToDatabaseFromString(
      "expected4[" +
        "(jay)-[jkb]->(bob);" +
        "]"
    );


    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getGraphCollectionByVariables("g0","g1","g4");


    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        PropertyValue city = vertexPojo.getProperties().get("city");
        return city != null && city.toString().equals("Leipzig");
      }
    };

    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>() {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        if(edgePojo.getLabel().equals("knows")){
          if(edgePojo.getPropertyValue("since").getInt() == 2016){
            return true;
          }

        }
        return false;
      }
    };

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
    input.apply(
      new ApplySubgraph<GraphHeadPojo, VertexPojo, EdgePojo>(
        vertexFilterFunction, edgeFilterFunction));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "(jay:Person {" +
        "name = \"Jay\", age=45, gender = \"f\", city = \"Leipzig\"})" +
        "g4:Community[" +
        "(jay)-[jkb:knows]->(bob);" +
        "(bob)-[blj:likes]->(jay);" +
        "]");

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(alice)-[akb]->(bob)-[bka]->(alice);" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[]"
    );

    loader.appendToDatabaseFromString(
      "expected4[" +
        "(jay)-[jkb]->(bob)-[blj]->(jay);" +
        "]"
    );

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getGraphCollectionByVariables("g0","g1","g4");


    FilterFunction<VertexPojo> vertexFilterFunction = new FilterFunction<VertexPojo>() {
      @Override
      public boolean filter(VertexPojo vertexPojo) throws Exception {
        PropertyValue city = vertexPojo.getProperties().get("city");
        return city != null && city.toString().equals("Leipzig");
      }
    };

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.apply(
        new ApplySubgraph<GraphHeadPojo, VertexPojo, EdgePojo>(
          vertexFilterFunction, null));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "expected0[" +
        "(eve)-[ekb]->(bob);" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected1[" +
        "(frank)-[fkc]->(carol);" +
        "(frank)-[fkd]->(dave);" +
        "]"
    );

    loader.appendToDatabaseFromString(
      "expected2[]"
    );

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getGraphCollectionByVariables("g0","g1","g2");


    FilterFunction<EdgePojo> edgeFilterFunction = new FilterFunction<EdgePojo>
      () {
      @Override
      public boolean filter(EdgePojo edgePojo) throws Exception {
        return edgePojo.getPropertyValue("since").getInt() == 2015;
      }
    };

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.apply(
        new ApplySubgraph<GraphHeadPojo, VertexPojo, EdgePojo>(
          null, edgeFilterFunction));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
  }
}
