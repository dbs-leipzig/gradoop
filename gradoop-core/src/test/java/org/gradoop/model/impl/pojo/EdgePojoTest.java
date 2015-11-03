package org.gradoop.model.impl.pojo;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.util.GConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EdgePojoTest {

  @Test
  public void createWithIDTest() {
    Long edgeId = 0L;
    Long sourceId = 23L;
    Long targetId = 42L;
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
    Long edgeId = 0L;
    String label = "A";
    Long sourceId = 23L;
    Long targetId = 42L;
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");
    Set<Long> graphs = Sets.newHashSet(0L, 1L);

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
    assertTrue(edge.getGraphs().contains(0L));
    assertTrue(edge.getGraphs().contains(1L));
  }

  @Test
  public void createWithMissingLabelTest() {
    EPGMEdge v = new EdgePojoFactory().createEdge(0L, 23L, 42L);
    assertThat(v.getLabel(), is(GConstants.DEFAULT_EDGE_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new EdgePojoFactory().createEdge(null, 23L, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullSourceIdTest() {
    new EdgePojoFactory().createEdge(0L, null, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullTargetIdTest() {
    new EdgePojoFactory().createEdge(0L, 23L, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    new EdgePojoFactory().createEdge(0L, "", 23L, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    new EdgePojoFactory().createEdge(0L, null, 23L, 42L);
  }
}