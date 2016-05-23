package org.gradoop.model.impl.operators.split;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.functions.UnaryFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SplitTest extends GradoopFlinkTestBase {

  @Test
  public void testSplit() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
          "input[" +
            "(v0 {key1 = 0})" +
            "(v1 {key1 = 1})" +
            "(v2 {key1 = 1})" +
            "(v3 {key1 = 0})" +
            "(v1)-[e1]->(v2)" +
            "(v3)-[e2]->(v0)" +
            "(v2)-[e3]->(v0)" +
          "]" +
          "graph1[" +
            "(v1)-[e1]->(v2)" +
          "]" +
          "graph2[" +
            "(v3)-[e2]->(v0)" +
          "]"
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getLogicalGraphByVariable("input");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.callForCollection(new Split<GraphHeadPojo,
            VertexPojo, EdgePojo>(new SelectKeyValues()));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("graph1", "graph2")));
  }

  @Test
  public void testSplit2() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
        "input[" +
        "(v0 {key1 = 0})" +
        "(v1 {key1 = 1})" +
        "(v2 {key1 = 1})" +
        "(v3 {key1 = 0})" +
        "(v4 {key1 = 2})" +
        "(v5 {key1 = 2})" +
        "(v1)-[e1]->(v2)" +
        "(v3)-[e2]->(v0)" +
        "(v2)-[e3]->(v0)" +
        "]" +
        "graph1[" +
        "(v1)-[e1]->(v2)" +
        "]" +
        "graph2[" +
        "(v3)-[e2]->(v0)" +
        "]" +
        "graph3[" +
        "(v4)" +
        "(v5)" +
        "]"
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getLogicalGraphByVariable("input");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.callForCollection(new Split<GraphHeadPojo,
        VertexPojo, EdgePojo>(new SelectKeyValues()));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      expectation = loader.getGraphCollectionByVariables(
      "graph1", "graph2", "graph3");

    collectAndAssertTrue(result.equalsByGraphElementIds(expectation));
  }


  @Test
  public void testSplitWithMultipleKeys() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
        "input[" +
        "(v0 {key1 = 0})" +
        "(v1 {key1 = 1})" +
        "(v2 {key1 = 1, key2 = 0})" +
        "(v3 {key1 = 0})" +
        "(v1)-[e1]->(v2)" +
        "(v3)-[e2]->(v0)" +
        "(v2)-[e3]->(v0)" +
        "]" +
        "graph1[" +
        "(v1)-[e1]->(v2)" +
        "]" +
        "graph2[" +
        "(v2)-[e3]->(v0)" +
        "(v3)-[e2]->(v0)" +
        "]"
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getLogicalGraphByVariable("input");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.callForCollection(new Split<GraphHeadPojo,
        VertexPojo, EdgePojo>(new SelectKeyValues()));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("graph1", "graph2")));
  }

  @Test
  public void testSplitWithSingleResultGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
          "g1:Persons [" +
          "(v0:Person {id = 0, author = \"value0\"})" +
          "(v1:Person {id = 0, author = \"value1\"})" +
          "(v2:Person {id = 0, author = \"value2\"})" +
          "(v3:Person {id = 0, author = \"value3\"})" +
          "(v0)-[e0:sameAs {id = 0, sim=\"0.91\"}]->(v1)" +
          "(v0)-[e1:sameAs {id = 1, sim=\"0.3\"}]->(v2)" +
          "(v2)-[e2:sameAs {id = 2, sim=\"0.1\"}]->(v1)" +
          "(v2)-[e3:sameAs {id = 3, sim=\"0.99\"}]->(v3)" +
          "]" +
          "g2 [" +
          "(v0)-[e0:sameAs {id = 0, sim=\"0.91\"}]->(v1)" +
          "(v0)-[e1:sameAs {id = 1, sim=\"0.3\"}]->(v2)" +
          "(v2)-[e2:sameAs {id = 2, sim=\"0.1\"}]->(v1)" +
          "(v2)-[e3:sameAs {id = 3, sim=\"0.99\"}]->(v3)" +
          "]"
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input =
      loader.getLogicalGraphByVariable("g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      input.splitBy("id");

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("g2")));
    collectAndAssertTrue(result.equalsByGraphElementData(
      loader.getGraphCollectionByVariables("g2")));
  }

  public static class SelectKeyValues
    implements UnaryFunction<VertexPojo, List<PropertyValue>>{
    @Override
    public List<PropertyValue> execute(VertexPojo entity) throws Exception {
      String key1 = "key1";
      String key2 = "key2";
      List<PropertyValue> valueList = new ArrayList<>();
      if (entity.hasProperty(key1)) {
        valueList.add(entity.getPropertyValue(key1));
      }
      if (entity.hasProperty(key2)) {
        valueList.add(entity.getProperties().get(key2));
      }

      return valueList;
    }
  }
}

