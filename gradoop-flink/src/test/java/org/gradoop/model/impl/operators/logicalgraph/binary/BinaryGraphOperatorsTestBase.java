package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

public class BinaryGraphOperatorsTestBase extends GradoopFlinkTestBase {

  public BinaryGraphOperatorsTestBase(TestExecutionMode mode) {
    super(mode);
  }

  protected void checkElementMatches(Set<EPGMGraphElement> inElements,
    Set<EPGMGraphElement> outElements) {
    for(EPGMGraphElement outElement : outElements) {
      boolean match = false;

      String elementClassName = outElement.getClass().getSimpleName();

      for(EPGMGraphElement inVertex : inElements) {
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
