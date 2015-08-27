package org.gradoop.model.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.GConstants;
import org.gradoop.model.EdgeData;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class DefaultEdgeDataTest {

  @Test
  public void createWithIDTest() {
    Long edgeId = 0L;
    Long sourceId = 23L;
    Long targetId = 42L;
    EdgeData e =
      new DefaultEdgeDataFactory().createEdgeData(edgeId, sourceId, targetId);
    assertThat(e.getId(), is(edgeId));
    assertThat(e.getSourceVertexId(), is(sourceId));
    assertThat(e.getTargetVertexId(), is(targetId));
    assertThat(e.getPropertyCount(), is(0));
    assertThat(e.getGraphCount(), is(0));
  }

  @Test
  public void createDefaultEdgeDataTest() {
    Long edgeId = 0L;
    String label = "A";
    Long sourceId = 23L;
    Long targetId = 42L;
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");
    Set<Long> graphs = Sets.newHashSet(0L, 1L);

    EdgeData edgeData = new DefaultEdgeDataFactory()
      .createEdgeData(edgeId, label, sourceId, targetId, props, graphs);

    assertThat(edgeData.getId(), is(edgeId));
    assertEquals(label, edgeData.getLabel());
    assertThat(edgeData.getSourceVertexId(), is(sourceId));
    assertThat(edgeData.getTargetVertexId(), is(targetId));
    assertThat(edgeData.getPropertyCount(), is(2));
    assertThat(edgeData.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(edgeData.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(edgeData.getGraphCount(), is(2));
    assertTrue(edgeData.getGraphs().contains(0L));
    assertTrue(edgeData.getGraphs().contains(1L));
  }

  @Test
  public void createWithMissingLabelTest() {
    EdgeData v = new DefaultEdgeDataFactory().createEdgeData(0L, 23L, 42L);
    assertThat(v.getLabel(), is(GConstants.DEFAULT_EDGE_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new DefaultEdgeDataFactory().createEdgeData(null, 23L, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullSourceIdTest() {
    new DefaultEdgeDataFactory().createEdgeData(0L, null, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullTargetIdTest() {
    new DefaultEdgeDataFactory().createEdgeData(0L, 23L, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    new DefaultEdgeDataFactory().createEdgeData(0L, "", 23L, 42L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    new DefaultEdgeDataFactory().createEdgeData(0L, null, 23L, 42L);
  }
}