package org.gradoop.model.impl.operators.matching.isomorphism.query;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TraversalQueryHandlerTest {

  static final String TEST_QUERY = "" +
    "(v1:A {ecc=2})" +
    "(v2:B {ecc=1})" +
    "(v3:B {ecc=2})" +
    "(v1)-[e1:a]->(v2)" +
    "(v2)-[e2:b]->(v3)" +
    "(v2)-[e3:a]->(v1)" +
    "(v3)-[e4:c]->(v3)";

  static final String EXPECTED_STEPS = "" +
    "1 2 0 false" +
    "1 1 2 true" +
    "2 3 2 true" +
    "0 0 1 true";

  static TraversalQueryHandler TRAVERSAL_QUERY_HANDLER =
    TraversalQueryHandler.fromString(TEST_QUERY);

  @Test
  public void testDFSTraversal() {
    Traversal traversal = TRAVERSAL_QUERY_HANDLER.getTraversal();
    List<Step> steps = traversal.getSteps();
    String result = "";
    for(Step step : steps) {
      result += step.toString();
    }
    assertEquals(result, EXPECTED_STEPS);
  }
}