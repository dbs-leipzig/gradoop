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
  public void splitOverlapTest() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
          "input[" +
            "(v0 {key = 0})" +
            "(v1 {key = 1})" +
            "(v2 {key = 1})" +
            "(v3 {key = 0})" +
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
            VertexPojo, EdgePojo>(new TestHelperFunction()));

    collectAndAssertTrue(result.equalsByGraphElementIds(
      loader.getGraphCollectionByVariables("graph1", "graph2")));
  }

  public static class TestHelperFunction
    implements UnaryFunction<VertexPojo, List<PropertyValue>>{
    @Override
    public List<PropertyValue> execute(VertexPojo entity) throws Exception {
      List<PropertyValue> valueList = new ArrayList<>();
      valueList.add(entity.getProperties().get("key"));
      return valueList;
    }
  }
}

