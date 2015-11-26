package org.gradoop.model.impl.pojo;

import com.google.common.collect.Maps;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.generators.TestSequenceIdGenerator;
import org.gradoop.util.GConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EdgePojoTest {

  @Test
  public void createWithIDTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    EPGMEdge e =
      new EdgePojoFactory().initEdge(edgeId, sourceId, targetId);
    assertThat(e.getId(), is(edgeId));
    assertThat(e.getSourceVertexId(), is(sourceId));
    assertThat(e.getTargetVertexId(), is(targetId));
    assertThat(e.getPropertyCount(), is(0));
    assertThat(e.getGraphCount(), is(0));
  }

  @Test
  public void createEdgePojoTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    GradoopIdSet graphIds = GradoopIdSet
      .fromExisting(idGen.createId(), idGen.createId());

    String label = "A";
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");

    EPGMEdge edge = new EdgePojoFactory()
      .initEdge(edgeId, label, sourceId, targetId, props, graphIds);

    assertThat(edge.getId(), is(edgeId));
    assertEquals(label, edge.getLabel());
    assertThat(edge.getSourceVertexId(), is(sourceId));
    assertThat(edge.getTargetVertexId(), is(targetId));
    assertThat(edge.getPropertyCount(), is(2));
    assertThat(edge.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(edge.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(edge.getGraphCount(), is(2));

    for(GradoopId graphId : graphIds) {
      assertTrue(edge.getGraphIds().contains(graphId));
    }
  }

  @Test
  public void createWithMissingLabelTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    EPGMEdge e =
      new EdgePojoFactory().initEdge(edgeId, sourceId, targetId);
    assertThat(e.getLabel(), is(GConstants.DEFAULT_EDGE_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().initEdge(null, sourceId, targetId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullSourceIdTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().initEdge(edgeId, null, targetId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullTargetIdTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    new EdgePojoFactory().initEdge(edgeId, sourceId, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().initEdge(edgeId, null, sourceId, targetId);
  }
}