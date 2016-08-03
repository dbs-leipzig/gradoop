package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.GradoopFlinkTestBase;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
