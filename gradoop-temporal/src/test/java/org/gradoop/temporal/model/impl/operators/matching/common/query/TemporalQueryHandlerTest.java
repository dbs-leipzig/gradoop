/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.operators.matching.common.query;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;


public class TemporalQueryHandlerTest {

  /**
   * copied from QueryHandlerTest (changed to TemporalQueryHandler). Ensure that this still works
   */
  static final String ECC_PROPERTY_KEY = "ecc";
  static final String TEST_QUERY = "" +
    "(v1:A {ecc : 2})" +
    "(v2:B {ecc : 1})" +
    "(v3:B {ecc : 2})" +
    "(v1)-[e1:a]->(v2)" +
    "(v2)-[e2:b]->(v3)" +
    "(v2)-[e3:a]->(v1)" +
    "(v3)-[e4:c]->(v3)";
  private static final GDLHandler GDL_HANDLER = new GDLHandler.Builder().buildFromString(TEST_QUERY);
  static TemporalQueryHandler QUERY_HANDLER;

  static {
    try {
      QUERY_HANDLER = new TemporalQueryHandler(TEST_QUERY);
    } catch (QueryContradictoryException e) {
      e.printStackTrace();
    }
  }

  private static <EL extends Element> boolean elementsEqual(List<EL> list, List<EL> expected) {
    boolean equal = list.size() == expected.size();

    if (equal) {
      list.sort(java.util.Comparator.comparingLong(Element::getId));
      expected.sort(java.util.Comparator.comparingLong(Element::getId));
      for (int i = 0; i < list.size(); i++) {
        if (!list.get(i).equals(expected.get(i))) {
          equal = false;
          break;
        }
      }
    }
    return equal;
  }

  @Test
  public void testSimplePredicates() {
    String testquery = "MATCH (a)-[e1:test]->(b) WHERE a.tx_from.before(b.tx_to)";
    TemporalQueryHandler handler = null;
    try {
      handler = new TemporalQueryHandler(testquery, new CNFPostProcessing(new ArrayList<>()));
    } catch (QueryContradictoryException e) {
      e.printStackTrace();
    }

    Predicate and = new And(
      new Comparison(new TimeSelector("b", "tx_to"),
        GT,
        new TimeSelector("a", "tx_from")),
      new Comparison(new PropertySelector("e1", "__label__"),
        Comparator.EQ,
        new Literal("test"))
    );
    and = new And(and, getOverlapsPredicate());
    assertPredicateEquals(and, handler.getPredicates());
  }

  @Test
  public void testGlobalPredicates() throws QueryContradictoryException {
    // no global predicates
    String testquery = "MATCH (a)-[e1:test]->(b) WHERE a.tx_to.before(b.val_to)";
    TimeSelector e1TxFrom = new TimeSelector("e1", TX_FROM);
    TimeSelector aTxFrom = new TimeSelector("a", TX_FROM);
    TimeSelector bTxFrom = new TimeSelector("b", TX_FROM);
    TimeSelector aTxTo = new TimeSelector("a", TX_TO);
    TimeSelector bTxTo = new TimeSelector("b", TX_TO);
    TimeSelector e1TxTo = new TimeSelector("e1", TX_TO);
    TimeSelector aValFrom = new TimeSelector("a", VAL_FROM);
    TimeSelector bValFrom = new TimeSelector("b", VAL_FROM);
    TimeSelector e1ValFrom = new TimeSelector("e1", VAL_FROM);
    TimeSelector aValTo = new TimeSelector("a", VAL_TO);
    TimeSelector bValTo = new TimeSelector("b", VAL_TO);
    TimeSelector e1ValTo = new TimeSelector("e1", VAL_TO);
    TemporalQueryHandler handler = new TemporalQueryHandler(testquery,
      new CNFPostProcessing(new ArrayList<>()));
    Predicate expectedPredicate = new And(
      new Comparison(aTxTo, LT, bValTo),
      new Comparison(new PropertySelector("e1", "__label__"),
        Comparator.EQ,
        new Literal("test"))
    );
    expectedPredicate = new And(expectedPredicate, getOverlapsPredicate());
    assertPredicateEquals(expectedPredicate, handler.getPredicates());

    // System.out.println("Here");

    // global and non-global predicates
    testquery = "MATCH (a)-[e1:test]->(b) WHERE tx_to.before(b.val_to) AND a.val_to.after(b.val_to)";
    handler = new TemporalQueryHandler(testquery, new CNFPostProcessing(new ArrayList<>()));
    MinTimePoint globalTxTo = new MinTimePoint(e1TxTo, aTxTo, bTxTo);
    MaxTimePoint globalTxFrom = new MaxTimePoint(e1TxFrom, aTxFrom, bTxFrom);
    MaxTimePoint globalValFrom = new MaxTimePoint(e1ValFrom, aValFrom, bValFrom);

    expectedPredicate = new And(
      new And(
        new And(
          new Comparison(globalTxTo, LT, bValTo),
          new Comparison(globalTxFrom, LTE, globalTxTo)),

        new Comparison(aValTo, GT, bValTo)
      ),
      new Comparison(new PropertySelector("e1", "__label__"),
        Comparator.EQ,
        new Literal("test"))
    );

    expectedPredicate = new And(expectedPredicate, getOverlapsPredicate());
    // System.out.println("handlerPredicates: \n" + handler.getPredicates() + "\n");
    assertPredicateEquals(expectedPredicate, handler.getPredicates());

    // only global
    testquery = "MATCH (a)-[e1]->(b) WHERE tx_to.before(b.val_to) AND a.val_to.after(val_to)";
    handler = new TemporalQueryHandler(testquery, new CNFPostProcessing(new ArrayList<>()));
    // System.out.println(handler.getPredicates());
    MinTimePoint globalValTo = new MinTimePoint(e1ValTo, aValTo, bValTo);

    expectedPredicate = new And(
      new And(
        new Comparison(globalTxTo, LT, bValTo),
        new Comparison(globalTxFrom, LTE, globalTxTo)),
      new And(
        new Comparison(aValTo, GT, globalValTo),
        new Comparison(globalValFrom, LTE, globalValTo))
    );
    // System.out.println("expected: \n" + handler.getPredicates() + "\n");
    expectedPredicate = new And(expectedPredicate, getOverlapsPredicate());
    assertPredicateEquals(expectedPredicate, handler.getPredicates());
  }

  private void assertPredicateEquals(Predicate expectedGDL, CNF result) {
    //// System.out.println(expectedGDL);
    CNF expected = QueryPredicate.createFrom(expectedGDL, new ComparableTPGMFactory()).asCNF();
    // System.out.println(expected);
    // System.out.println(result);
    equalCNFs(expected, result);
  }

  private void equalCNFs(CNF a, CNF b) {
    assertEquals(a.getPredicates().size(), b.getPredicates().size());
    for (CNFElement aElement : a.getPredicates()) {
      ComparisonExpression aComp = aElement.getPredicates().get(0);
      boolean foundA = false;
      for (CNFElement bElement : b.getPredicates()) {
        ComparisonExpression bComp = bElement.getPredicates().get(0);
        if (comparisonEqual(aComp, bComp)) {
          foundA = true;
        }
      }
      assertTrue(foundA);
    }
  }

  private boolean comparisonEqual(ComparisonExpression a, ComparisonExpression b) {
    QueryComparable lhsA = a.getLhs();
    QueryComparable rhsA = a.getRhs();
    QueryComparable lhsB = b.getLhs();
    QueryComparable rhsB = b.getRhs();
    Comparator compA = a.getComparator();
    Comparator compB = b.getComparator();

    if (lhsA.equals(lhsB) && (compA.equals(compB)) && lhsA.equals(lhsB)) {
      return true;
    }

    Comparator compASwitched = a.switchSides().getComparator();
    if (lhsA.equals(rhsB) && (compASwitched.equals(compB)) && rhsA.equals(lhsB)) {
      return true;
    }
    return false;
  }


  // returns the overlaps predicate for the edge (a)-[e]->(b)
  private Predicate getOverlapsPredicate() {
    TimeSelector eFrom = new TimeSelector("e1", VAL_FROM);
    TimeSelector eTo = new TimeSelector("e1", TimeSelector.TimeField.VAL_TO);

    TimeSelector sFrom = new TimeSelector("a", VAL_FROM);
    TimeSelector sTo = new TimeSelector("a", TimeSelector.TimeField.VAL_TO);

    TimeSelector tFrom = new TimeSelector("b", VAL_FROM);
    TimeSelector tTo = new TimeSelector("b", TimeSelector.TimeField.VAL_TO);

    Predicate overlaps = new Comparison(
      new MaxTimePoint(eFrom, sFrom, tFrom), LTE, new MinTimePoint(eTo, sTo, tTo)
    );
    return overlaps;
  }


  // also copied from EPGM tests

  @Test
  public void testGetVertexCount() {
    assertEquals(3, QUERY_HANDLER.getVertexCount());
  }

  @Test
  public void testGetEdgeCount() {
    assertEquals(4, QUERY_HANDLER.getEdgeCount());
  }


  @Test
  public void testIsSingleVertexGraph() {
    assertFalse(QUERY_HANDLER.isSingleVertexGraph());
    assertTrue(new QueryHandler("(v0)").isSingleVertexGraph());
  }

  @Test
  public void testIsVertex() {
    assertTrue(QUERY_HANDLER.isVertex("v1"));
    assertFalse(QUERY_HANDLER.isVertex("e1"));
  }

  @Test
  public void testIsEdge() {
    assertTrue(QUERY_HANDLER.isEdge("e1"));
    assertFalse(QUERY_HANDLER.isEdge("v1"));
  }

  @Test
  public void testGetVertexById() throws Exception {
    Vertex expected = GDL_HANDLER.getVertexCache().get("v1");
    assertTrue(QUERY_HANDLER.getVertexById(expected.getId()).equals(expected));
  }

  @Test
  public void testGetEdgeById() throws Exception {
    Edge expected = GDL_HANDLER.getEdgeCache().get("e1");
    assertTrue(QUERY_HANDLER.getEdgeById(expected.getId()).equals(expected));
  }

  @Test
  public void testGetVertexByVariable() throws Exception {
    Vertex expected = GDL_HANDLER.getVertexCache().get("v1");
    assertEquals(QUERY_HANDLER.getVertexByVariable("v1"), expected);
    assertNotEquals(QUERY_HANDLER.getVertexByVariable("v2"), expected);
  }

  @Test
  public void testGetEdgeByVariable() throws Exception {
    Edge expected = GDL_HANDLER.getEdgeCache().get("e1");
    assertEquals(QUERY_HANDLER.getEdgeByVariable("e1"), expected);
    assertNotEquals(QUERY_HANDLER.getEdgeByVariable("e2"), expected);
  }
}
