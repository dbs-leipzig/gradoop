package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.operators.distinction.functions.CountGraphHeads;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class GroupByIsomorphismTest extends DistinctByIsomorphismTestBase {

  @Test
  public void execute() throws Exception {
    GraphCollection collection = getTestCollection();

    String propertyKey = "count";

    GraphHeadReduceFunction countFunc = new CountGraphHeads(propertyKey);

    collection = collection.groupByIsomorphism(countFunc);

    List<GraphHead> graphHeads = collection.getGraphHeads().collect();

    assertEquals(3, graphHeads.size());

    for (GraphHead graphHead : graphHeads) {
      assertTrue(graphHead.hasProperty(propertyKey));
      int count = graphHead.getPropertyValue(propertyKey).getInt();

      String label = graphHead.getLabel();

      if (label.equals("G")) {
        assertEquals(1, count);
      } else {
        assertEquals(2, count);
      }
    }
  }
}