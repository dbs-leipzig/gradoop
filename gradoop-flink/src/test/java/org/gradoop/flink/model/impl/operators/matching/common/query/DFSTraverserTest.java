package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

public class DFSTraverserTest {

  @Test
  public void testTraverse() {
    RootedTraverser traverser = new DFSTraverser();
    traverser.setQueryHandler(new QueryHandler(QueryHandlerTest.TEST_QUERY));

    TraversalCode result = traverser.traverse();

    assertTrue(Iterables.elementsEqual(result.getSteps(), Lists.newArrayList(
      new Step(0L, 2L, 1L, false),
      new Step(1L, 0L, 0L, false),
      new Step(1L, 1L, 2L, true),
      new Step(2L, 3L, 2L, true)
    )));
  }

  @Test
  public void testTraverseFromRootVertex() {
    RootedTraverser traverser = new DFSTraverser();
    traverser.setQueryHandler(new QueryHandler(QueryHandlerTest.TEST_QUERY));

    TraversalCode result = traverser.traverse(2L);

    assertTrue(Iterables.elementsEqual(result.getSteps(), Lists.newArrayList(
      new Step(2L, 3L, 2L, true),
      new Step(2L, 1L, 1L, false),
      new Step(1L, 0L, 0L, false),
      new Step(0L, 2L, 1L, false) 
    )));
  }

  @Test
  public void testTraverseLoop() {
    RootedTraverser traverser = new DFSTraverser();
    traverser.setQueryHandler(new QueryHandler("(v0)-->(v0)"));

    TraversalCode result = traverser.traverse(0L);

    assertTrue(Iterables.elementsEqual(result.getSteps(), Lists.newArrayList(
      new Step(0L, 0L, 0L, true)
    )));
  }
}