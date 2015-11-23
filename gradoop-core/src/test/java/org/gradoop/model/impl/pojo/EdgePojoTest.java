package org.gradoop.model.impl.pojo;

import com.google.common.collect.Maps;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdGenerator;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.SequenceIdGenerator;
import org.gradoop.util.GConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EdgePojoTest {

  @Test
  public void createWithIDTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    EPGMEdge e =
      new EdgePojoFactory().createEdge(edgeId, sourceId, targetId);
    assertThat(e.getId(), is(edgeId));
    assertThat(e.getSourceVertexId(), is(sourceId));
    assertThat(e.getTargetVertexId(), is(targetId));
    assertThat(e.getPropertyCount(), is(0));
    assertThat(e.getGraphCount(), is(0));
  }

  @Test
  public void createEdgePojoTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();

    String label = "A";
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");
    GradoopIdSet graphs = GradoopIdSet.fromLongs(0L, 1L);

    EPGMEdge edge = new EdgePojoFactory()
      .createEdge(edgeId, label, sourceId, targetId, props, graphs);

    assertThat(edge.getId(), is(edgeId));
    assertEquals(label, edge.getLabel());
    assertThat(edge.getSourceVertexId(), is(sourceId));
    assertThat(edge.getTargetVertexId(), is(targetId));
    assertThat(edge.getPropertyCount(), is(2));
    assertThat(edge.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(edge.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(edge.getGraphCount(), is(2));
    assertTrue(edge.getGraphIds().contains(GradoopId.fromLong(0L)));
    assertTrue(edge.getGraphIds().contains(GradoopId.fromLong(1L)));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    EPGMEdge e =
      new EdgePojoFactory().createEdge(edgeId, sourceId, targetId);
    assertThat(e.getLabel(), is(GConstants.DEFAULT_EDGE_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().createEdge(null, sourceId, targetId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullSourceIdTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().createEdge(edgeId, null, targetId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullTargetIdTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    new EdgePojoFactory().createEdge(edgeId, sourceId, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().createEdge(edgeId, "", sourceId, targetId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId edgeId = idGen.createId();
    GradoopId sourceId = idGen.createId();
    GradoopId targetId = idGen.createId();
    new EdgePojoFactory().createEdge(edgeId, null, sourceId, targetId);
  }
}