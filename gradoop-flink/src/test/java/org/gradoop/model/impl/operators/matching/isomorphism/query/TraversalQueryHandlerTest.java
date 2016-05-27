package org.gradoop.model.impl.operators.matching.isomorphism.query;

import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.GDLHandler.Builder;

import java.util.List;

public class TraversalQueryHandlerTest {

  static final String TEST_QUERY = "" +
    "(v1:A {ecc=2})" +
    "(v2:B {ecc=1})" +
    "(v3:B {ecc=2})" +
    "(v1)-[e1:a]->(v2)" +
    "(v2)-[e2:b]->(v3)" +
    "(v2)-[e3:a]->(v1)" +
    "(v3)-[e4:c]->(v3)";

  static TraversalQueryHandler TRAVERSAL_QUERY_HANDLER =
    TraversalQueryHandler.fromString(TEST_QUERY);

  @Test
  public void testDFSTraversal() {
    Traversal traversal = TRAVERSAL_QUERY_HANDLER.getTraversal();
    List<Step> steps = traversal.getSteps();
    for(Step step : steps) {
      System.out.println(step.toString());
    }
  }
}