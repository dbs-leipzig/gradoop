package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class AddGraphElementToNewGraphTest {
  @Test
  public void testVariableMappingCreation() throws Exception {
    AddGraphElementToNewGraph<Vertex> udf =
      new AddGraphElementToNewGraph<>(new GraphHeadFactory(), "a");

    Vertex vertex = new VertexFactory().createVertex();
    Tuple2<Vertex, GraphHead> result = udf.map(vertex);

    assertTrue(result.f1.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
    Map<PropertyValue, PropertyValue> variableMapping =
      result.f1.getPropertyValue(PatternMatching.VARIABLE_MAPPING_KEY).getMap();

    assertEquals(vertex.getId(), variableMapping.get(PropertyValue.create("a")).getGradoopId());
  }
}