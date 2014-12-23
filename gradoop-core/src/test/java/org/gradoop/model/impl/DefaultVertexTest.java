package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class DefaultVertexTest {

  @Test
  public void createWithIDTest() {
    Long vertexID = 0L;
    Vertex v = VertexFactory.createDefaultVertexWithID(vertexID);
    assertThat(v.getID(), is(vertexID));
    assertThat(v.getLabelCount(), is(0));
    assertThat(v.getGraphCount(), is(0));
    assertThat(v.getOutgoingDegree(), is(0));
    assertThat(v.getIncomingDegree(), is(0));
    assertThat(v.getDegree(), is(0));
  }

  @Test
  public void createWithIDOutgoingEdgesTest() {
    Long vertexID = 0L;
    Long otherID1 = 1L;
    Long otherID2 = 2L;
    List<Edge> outgoingEdges = Lists.newArrayListWithCapacity(2);
    Edge e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
    outgoingEdges.add(e1);
    Edge e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
    outgoingEdges.add(e2);

    Vertex v = VertexFactory.createDefaultVertexWithOutgoingEdges(vertexID,
      outgoingEdges);

    assertThat(v.getID(), is(vertexID));
    assertThat(v.getOutgoingDegree(), is(2));
    assertThat(v.getDegree(), is(2));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));
  }

  @Test
  public void createWithIDEdgesTest() {
    Long vertexID = 0L;
    Long otherID1 = 1L;
    Long otherID2 = 2L;
    Long otherID3 = 3L;
    Long otherID4 = 4L;
    List<Edge> outgoingEdges = Lists.newArrayListWithCapacity(2);
    Edge e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
    outgoingEdges.add(e1);
    Edge e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
    outgoingEdges.add(e2);

    List<Edge> incomingEdges = Lists.newArrayListWithCapacity(2);
    Edge e3 = EdgeFactory.createDefaultEdge(otherID3, otherID3);
    incomingEdges.add(e3);
    Edge e4 = EdgeFactory.createDefaultEdge(otherID4, otherID4);
    incomingEdges.add(e4);

    Vertex v = VertexFactory.createDefaultVertexWithEdges(vertexID,
      outgoingEdges, incomingEdges);
    assertThat(v.getID(), is(vertexID));

    assertThat(v.getOutgoingDegree(), is(2));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));

    assertThat(v.getIncomingDegree(), is(2));
    assertTrue(Lists.newArrayList(v.getIncomingEdges()).contains(e3));
    assertTrue(Lists.newArrayList(v.getIncomingEdges()).contains(e4));

    assertThat(v.getDegree(), is(4));
  }

  @Test
  public void createWithIDLabelsEdgesTest() {
    Long vertexID = 0L;
    Long otherID1 = 1L;
    Long otherID2 = 2L;

    String label1 = "A";
    String label2 = "B";
    List<String> labels = Lists.newArrayList(label1, label2);
    List<Edge> outgoingEdges = Lists.newArrayListWithCapacity(2);
    Edge e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
    outgoingEdges.add(e1);
    Edge e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
    outgoingEdges.add(e2);

    Vertex v = VertexFactory.createDefaultVertexWithLabels(vertexID, labels,
      outgoingEdges);

    assertThat(v.getID(), is(vertexID));

    assertThat(v.getLabelCount(), is(2));
    assertTrue(Lists.newArrayList(v.getLabels()).contains(label1));
    assertTrue(Lists.newArrayList(v.getLabels()).contains(label2));

    assertThat(v.getOutgoingDegree(), is(2));
    assertThat(v.getDegree(), is(2));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));
  }

  @Test
  public void createDefaultVertexTest() {
    Long vertexID = 0L;
    Long otherID1 = 1L;
    Long otherID2 = 2L;
    Long otherID3 = 3L;
    Long otherID4 = 4L;

    String label1 = "A";
    String label2 = "B";
    List<String> labels = Lists.newArrayList(label1, label2);

    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");

    List<Edge> outgoingEdges = Lists.newArrayListWithCapacity(2);
    Edge e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
    outgoingEdges.add(e1);
    Edge e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
    outgoingEdges.add(e2);

    List<Edge> incomingEdges = Lists.newArrayListWithCapacity(2);
    Edge e3 = EdgeFactory.createDefaultEdge(otherID3, otherID3);
    incomingEdges.add(e3);
    Edge e4 = EdgeFactory.createDefaultEdge(otherID4, otherID4);
    incomingEdges.add(e4);

    List<Long> graphs = Lists.newArrayList(0L, 1L);

    Vertex v = VertexFactory.createDefaultVertex(vertexID, labels, props,
      outgoingEdges, incomingEdges, graphs);

    assertThat(v.getLabelCount(), is(2));
    assertTrue(Lists.newArrayList(v.getLabels()).contains(label1));
    assertTrue(Lists.newArrayList(v.getLabels()).contains(label2));

    assertThat(v.getPropertyCount(), is(2));
    assertEquals("v1", v.getProperty("k1"));
    assertEquals("v2", v.getProperty("k2"));

    assertThat(v.getID(), is(vertexID));
    List<Edge> e = Lists.newArrayList(v.getOutgoingEdges());
    assertThat(e.size(), is(2));
    assertTrue(e.contains(e1));
    assertTrue(e.contains(e2));
    e = Lists.newArrayList(v.getIncomingEdges());
    assertThat(e.size(), is(2));
    assertTrue(e.contains(e3));
    assertTrue(e.contains(e4));

    assertThat(v.getGraphCount(), is(2));
    assertTrue(Lists.newArrayList(v.getGraphs()).contains(0L));
    assertTrue(Lists.newArrayList(v.getGraphs()).contains(1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithMissingIDTest() {
    VertexFactory.createDefaultVertexWithID(null);
  }
}
