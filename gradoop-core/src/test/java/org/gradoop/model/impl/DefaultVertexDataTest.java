//package org.gradoop.model.impl;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.VertexData;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.Map;
//
//import static org.hamcrest.core.Is.is;
//import static org.junit.Assert.*;
//
//public class DefaultVertexDataTest {
//
//  @Test
//  public void createWithIDTest() {
//    Long vertexID = 0L;
//    VertexData v = VertexFactory.createDefaultVertexWithID(vertexID);
//    assertThat(v.getId(), is(vertexID));
//    assertThat(v.getGraphCount(), is(0));
//  }
//
//  @Test
//  public void createWithIDOutgoingEdgesTest() {
//    Long vertexID = 0L;
//    Long otherID1 = 1L;
//    Long otherID2 = 2L;
//    List<EdgeData> outgoingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
//    outgoingEdgeDatas.add(e1);
//    EdgeData e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
//    outgoingEdgeDatas.add(e2);
//
//    VertexData v = VertexFactory.createDefaultVertexWithOutgoingEdges(vertexID,
//      outgoingEdgeDatas);
//
//    assertThat(v.getId(), is(vertexID));
//    assertThat(v.getOutgoingDegree(), is(2));
//    assertThat(v.getDegree(), is(2));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));
//  }
//
//  @Test
//  public void createWithIDEdgesTest() {
//    Long vertexID = 0L;
//    Long otherID1 = 1L;
//    Long otherID2 = 2L;
//    Long otherID3 = 3L;
//    Long otherID4 = 4L;
//    List<EdgeData> outgoingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
//    outgoingEdgeDatas.add(e1);
//    EdgeData e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
//    outgoingEdgeDatas.add(e2);
//
//    List<EdgeData> incomingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e3 = EdgeFactory.createDefaultEdge(otherID3, otherID3);
//    incomingEdgeDatas.add(e3);
//    EdgeData e4 = EdgeFactory.createDefaultEdge(otherID4, otherID4);
//    incomingEdgeDatas.add(e4);
//
//    VertexData v = VertexFactory.createDefaultVertexWithEdges(vertexID,
//      outgoingEdgeDatas, incomingEdgeDatas);
//    assertThat(v.getId(), is(vertexID));
//
//    assertThat(v.getOutgoingDegree(), is(2));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));
//
//    assertThat(v.getIncomingDegree(), is(2));
//    assertTrue(Lists.newArrayList(v.getIncomingEdges()).contains(e3));
//    assertTrue(Lists.newArrayList(v.getIncomingEdges()).contains(e4));
//
//    assertThat(v.getDegree(), is(4));
//  }
//
//  @Test
//  public void createWithIDLabelsEdgesTest() {
//    Long vertexID = 0L;
//    Long otherID1 = 1L;
//    Long otherID2 = 2L;
//
//    String label = "A";
//
//    List<EdgeData> outgoingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
//    outgoingEdgeDatas.add(e1);
//    EdgeData e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
//    outgoingEdgeDatas.add(e2);
//
//    VertexData v = VertexFactory.createDefaultVertexWithLabel(vertexID, label,
//      outgoingEdgeDatas);
//
//    assertThat(v.getId(), is(vertexID));
//    assertEquals(label, v.getLabel());
//    assertThat(v.getOutgoingDegree(), is(2));
//    assertThat(v.getDegree(), is(2));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e1));
//    assertTrue(Lists.newArrayList(v.getOutgoingEdges()).contains(e2));
//  }
//
//  @Test
//  public void addAndResetGraphsTest() {
//    Long vertexID0 = 0L;
//    Long vertexID1 = 1L;
//    Long vertexID2 = 2L;
//    List<Long> tmpGraphs = Lists.newArrayListWithCapacity(2);
//    tmpGraphs.add(vertexID0);
//    tmpGraphs.add(vertexID1);
//
//    //tests
//    VertexData v0 = VertexFactory.createDefaultVertexWithID(vertexID0);
//    v0.addGraphs(tmpGraphs);
//    assertEquals(2, v0.getGraphCount());
//    //add already contained element
//    v0.addGraph(vertexID0);
//    assertEquals(2, v0.getGraphCount());
//    //add new element
//    v0.addGraph(vertexID2);
//    assertEquals(3, v0.getGraphCount());
//    //reset
//    v0.resetGraphs();
//    assertEquals(0, v0.getGraphCount());
//
//    //defaultVertex, optionally
//    VertexData v1 = VertexFactory.createDefaultVertex(vertexID1, null, null,
//      null, null, tmpGraphs);
//    assertEquals(2, v1.getGraphCount());
//    v1.resetGraphs();
//    assertEquals(0, v1.getGraphCount());
//  }
//
//  @Test
//  public void createDefaultVertexTest() {
//    Long vertexID = 0L;
//    Long otherID1 = 1L;
//    Long otherID2 = 2L;
//    Long otherID3 = 3L;
//    Long otherID4 = 4L;
//
//    String label = "A";
//
//    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
//    props.put("k1", "v1");
//    props.put("k2", "v2");
//
//    List<EdgeData> outgoingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e1 = EdgeFactory.createDefaultEdge(otherID1, otherID1);
//    outgoingEdgeDatas.add(e1);
//    EdgeData e2 = EdgeFactory.createDefaultEdge(otherID2, otherID2);
//    outgoingEdgeDatas.add(e2);
//
//    List<EdgeData> incomingEdgeDatas = Lists.newArrayListWithCapacity(2);
//    EdgeData e3 = EdgeFactory.createDefaultEdge(otherID3, otherID3);
//    incomingEdgeDatas.add(e3);
//    EdgeData e4 = EdgeFactory.createDefaultEdge(otherID4, otherID4);
//    incomingEdgeDatas.add(e4);
//
//    List<Long> graphs = Lists.newArrayList(0L, 1L);
//
//    VertexData v = VertexFactory.createDefaultVertex(vertexID, label, props,
//      outgoingEdgeDatas, incomingEdgeDatas, graphs);
//
//    assertEquals(label, v.getLabel());
//    assertThat(v.getPropertyCount(), is(2));
//    assertEquals("v1", v.getProperty("k1"));
//    assertEquals("v2", v.getProperty("k2"));
//
//    assertThat(v.getId(), is(vertexID));
//    List<EdgeData> e = Lists.newArrayList(v.getOutgoingEdges());
//    assertThat(e.size(), is(2));
//    assertTrue(e.contains(e1));
//    assertTrue(e.contains(e2));
//    e = Lists.newArrayList(v.getIncomingEdges());
//    assertThat(e.size(), is(2));
//    assertTrue(e.contains(e3));
//    assertTrue(e.contains(e4));
//
//    assertThat(v.getGraphCount(), is(2));
//    assertTrue(Lists.newArrayList(v.getGraphs()).contains(0L));
//    assertTrue(Lists.newArrayList(v.getGraphs()).contains(1L));
//  }
//
//  @Test(expected = IllegalArgumentException.class)
//  public void createWithMissingIDTest() {
//    VertexFactory.createDefaultVertexWithID(null);
//  }
//}
