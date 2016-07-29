package org.gradoop.model.impl.pojo;

import org.gradoop.GradoopTestUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;
import org.junit.Test;

import static org.gradoop.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class PojoSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexSerialization() throws Exception {
    Vertex vertexIn = new VertexPojoFactory().createVertex(
      "Person",
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get()));

    assertEquals("Vertex POJOs were not equal",
      vertexIn, writeAndRead(vertexIn));
  }

  @Test
  public void testEdgeSerialization() throws Exception {
    Edge edgeIn = new EdgePojoFactory().createEdge(
      "knows",
      GradoopId.get(),
      GradoopId.get(),
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get()));

    assertEquals("Edge POJOs were not equal",
      edgeIn, writeAndRead(edgeIn));
  }

  @Test
  public void testGraphHeadSerialization() throws Exception {
    GraphHead graphHeadIn = new GraphHeadPojoFactory().createGraphHead(
      "Community",
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES)
    );

    assertEquals("GraphHead POJOs were not equal",
      graphHeadIn, writeAndRead(graphHeadIn));
  }


}
