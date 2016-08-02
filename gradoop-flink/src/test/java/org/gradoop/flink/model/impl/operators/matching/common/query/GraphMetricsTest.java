package org.gradoop.flink.model.impl.operators.matching.common.query;

import org.junit.Test;

import java.util.Map;

import static org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandlerTest.ECC_PROPERTY_KEY;
import static org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandlerTest.QUERY_HANDLER;
import static org.junit.Assert.assertEquals;

public class GraphMetricsTest {

  @Test
  public void testGetDiameter() throws Exception {
    assertEquals(2, GraphMetrics.getDiameter(QUERY_HANDLER));
  }

  @Test
  public void testGetRadius() throws Exception {
    assertEquals(1, GraphMetrics.getRadius(QUERY_HANDLER));
  }

  @Test
  public void testGetEccentricity() throws Exception {
    Map<Long, Integer> eccentricity = GraphMetrics
      .getEccentricity(QUERY_HANDLER);

    for (Map.Entry<Long, Integer> ecc : eccentricity.entrySet()) {
      Integer expected = (Integer) QUERY_HANDLER
        .getVertexById(ecc.getKey())
        .getProperties().get(ECC_PROPERTY_KEY);
      assertEquals(expected, ecc.getValue());
    }
  }
}