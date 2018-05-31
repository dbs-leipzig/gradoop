package org.gradoop.flink.model.impl.functions.epgm;

import static org.junit.Assert.assertEquals;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.junit.Test;

public class RenameLabelTest extends GradoopFlinkTestBase {

  @Test
  public void testGraphHead() {

    GradoopId graphId = GradoopId.get();
    String label = "A";

    EPGMGraphHead graphHead = new GraphHeadFactory().initGraphHead(graphId, label);

    String newLabel = "B";

    TransformationFunction<EPGMGraphHead> renameFunction = new RenameLabel<>(label, newLabel);

    renameFunction.apply(graphHead, graphHead);

    assertEquals(newLabel, graphHead.getLabel());
  }

  @Test
  public void testEdge() {

    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    EPGMEdge edge = new EdgeFactory().initEdge(edgeId, label, sourceId, targetId);

    String newLabel = "B";

    TransformationFunction<EPGMEdge> renameFunction = new RenameLabel<>(label, newLabel);

    renameFunction.apply(edge, edge);

    assertEquals(newLabel, edge.getLabel());
  }

  @Test
  public void testVertex() {

    GradoopId vertexId = GradoopId.get();
    String label = "A";

    EPGMVertex vertex = new VertexFactory().initVertex(vertexId, label);

    String newLabel = "B";

    TransformationFunction<EPGMVertex> renameFunction = new RenameLabel<>(label, newLabel);

    renameFunction.apply(vertex, vertex);
 
    assertEquals(newLabel, vertex.getLabel());
  }
}
