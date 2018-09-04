package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class GraphElementTest {

  @Test
  public void testAddGraphIdNoGraphIds() {
    GraphElement graphElementMock = mock(GraphElement.class, Mockito.CALLS_REAL_METHODS);

    GradoopId id = new GradoopId();
    graphElementMock.addGraphId(id);

    assertNotNull(graphElementMock.getGraphIds());
  }

  @Test
  public void testResetGraphIds() {
    Properties propertiesMock = mock(Properties.class);
    GradoopIdSet idSet = new GradoopIdSet();
    idSet.add(new GradoopId());

    GraphElement graphElementMock = mock(GraphElement.class, withSettings()
    .useConstructor(new GradoopId(), "someLabel", propertiesMock, idSet)
    .defaultAnswer(CALLS_REAL_METHODS));

    graphElementMock.resetGraphIds();

    assertTrue(graphElementMock.getGraphIds().isEmpty());
  }
}
