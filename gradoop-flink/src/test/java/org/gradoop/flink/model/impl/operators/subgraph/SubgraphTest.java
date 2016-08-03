package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
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
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave);" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave);" +
      "(eve)-[eka]->(alice);" +
      "(eve)-[ekb]->(bob);" +
      "(frank)-[fkc]->(carol);" +
      "(frank)-[fkd]->(dave);" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Person");
      }
    };

    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        return edgePojo.getLabel().equals("knows");
      }
    };

    LogicalGraph output = input
      .subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice);(bob);(carol);(dave);(eve);(frank);" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Person");
      }
    };

    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        return edgePojo.getLabel().equals("friendOf");
      }
    };

    LogicalGraph output = input.subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph which is empty.
   *
   * @throws Exception
   */
  @Test
  public void testEmptySubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("User");
      }
    };

    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        return edgePojo.getLabel().equals("friendOf");
      }
    };

    LogicalGraph output = input
      .subgraph(vertexFilterFunction, edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs);" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop);" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        return vertexPojo.getLabel().equals("Forum")
          || vertexPojo.getLabel().equals("Tag") ;
      }
    };

    LogicalGraph output = input.vertexInducedSubgraph(vertexFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs);" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop);" +
      "]");

    LogicalGraph input = loader.getDatabase().getDatabaseGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        return edgePojo.getLabel().equals("hasTag");
      }
    };

    LogicalGraph output = input.edgeInducedSubgraph(edgeFilterFunction);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testCollectionSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

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


    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g4");


    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        PropertyValue city = vertexPojo.getProperties().get("city");
        return city != null && city.toString().equals("Leipzig");
      }
    };

    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        if(edgePojo.getLabel().equals("knows")){
          if(edgePojo.getPropertyValue("since").getInt() == 2016){
            return true;
          }

        }
        return false;
      }
    };

    GraphCollection result = input
      .apply(new ApplySubgraph(vertexFilterFunction, edgeFilterFunction));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

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

    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g4");


    FilterFunction<Vertex> vertexFilterFunction = new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertexPojo) throws Exception {
        PropertyValue city = vertexPojo.getProperties().get("city");
        return city != null && city.toString().equals("Leipzig");
      }
    };

    GraphCollection result = input
      .apply(new ApplySubgraph(vertexFilterFunction, null));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected4")));
  }

  @Test
  public void testCollectionEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

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

    GraphCollection input = loader.getGraphCollectionByVariables("g0","g1","g2");


    FilterFunction<Edge> edgeFilterFunction = new FilterFunction<Edge>() {
      @Override
      public boolean filter(Edge edgePojo) throws Exception {
        return edgePojo.getPropertyValue("since").getInt() == 2015;
      }
    };

    GraphCollection result = input
      .apply(new ApplySubgraph(null, edgeFilterFunction));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
    collectAndAssertTrue(result.equalsByGraphData(
      loader.getGraphCollectionByVariables(
        "expected0", "expected1", "expected2")));
  }
}
