package org.gradoop.model.impl.operators.base;

import org.gradoop.common.model.GradoopFlinkTestBase;
import org.gradoop.common.model.api.entities.GraphElement;

import java.util.Set;

import static org.junit.Assert.*;

public class BinaryGraphOperatorsTestBase extends GradoopFlinkTestBase {

  protected void checkElementMatches(Set<GraphElement> inElements,
    Set<GraphElement> outElements) {
    for(GraphElement outElement : outElements) {
      boolean match = false;

      String elementClassName = outElement.getClass().getSimpleName();

      for(GraphElement inVertex : inElements) {
        if (outElement.getId().equals(inVertex.getId())) {
          assertEquals(
            "wrong number of graphs for " + elementClassName,
            inVertex.getGraphCount() + 1,
            outElement.getGraphCount()
          );
          match = true;
          break;
        }
      }
      assertTrue("expected " + elementClassName + " not found",match);
    }
  }
}
