package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class DistinctByIsomorphismTest extends DistinctByIsomorphismTestBase {

  @Test
  public void execute() throws Exception {
    GraphCollection collection = getTestCollection();

    collection = collection.distinctById();

    assertEquals(5, collection.getGraphHeads().count());

    collection = collection.distinctByIsomorphism();

    assertEquals(3, collection.getGraphHeads().count());

  }
}