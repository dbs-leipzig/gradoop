/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.patternmatching;

import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;

/**
 * Set of benchmark queries
 */
public class Queries {

  /**
   * Query representation
   */
  public static class Query {
    /**
     * {@link TraversalCode} for the query
     */
    private TraversalCode traversalCode;
    /**
     * Number of query vertices
     */
    private int vertexCount;
    /**
     * Number of query edges
     */
    private int edgeCount;

    /**
     * Constructor
     *
     * @param traversalCode traversal code
     * @param vertexCount   number of query vertices
     * @param edgeCount     number of query edges
     */
    Query(TraversalCode traversalCode, int vertexCount, int edgeCount) {
      this.traversalCode = traversalCode;
      this.vertexCount = vertexCount;
      this.edgeCount = edgeCount;
    }

    public TraversalCode getTraversalCode() {
      return traversalCode;
    }

    public int getVertexCount() {
      return vertexCount;
    }

    public int getEdgeCount() {
      return edgeCount;
    }
  }

  /**
   * (a)-->(b)
   *
   * (0)-0->(1)
   *
   * @return query q0
   */
  public static Query q0() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, true)); // (a)-->(b)
    return new Query(tc, 2, 1);
  }

  /**
   * (a)-->(b)-->(a)
   *
   * (0)-0->(1)-1->(0)
   *
   * @return query q1
   */
  public static Query q1() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, true)); // (a)-->(b)
    tc.add(new Step(1L, 1L, 0L, true)); // (b)-->(a)
    return new Query(tc, 2, 2);
  }

  /**
   * (c)<--(a)-->(b)
   *
   * (0)<-0-(1)-1->(2)
   *
   * static:
   * TraversalCode{steps=[(1,0,0,true), (1,1,2,true)]}
   * embeddingCount = 67.833.471
   *
   * GDL:
   * TraversalCode{steps=[(0,0,1,false), (1,1,2,true)]}
   * embeddingCount = 62.728.432
   *
   *
   * @return query q2
   */
  public static Query q2() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(1L, 0L, 0L, true)); // (a)-->(c)
    tc.add(new Step(1L, 1L, 2L, true)); // (a)-->(b)
    return new Query(tc, 3, 2);
  }

  /**
   * (a)<--(b)-->(c)-->(a)
   *
   * (0)<-0-(1)-1->(2)-2->(0)
   *
   * @return query q3
   */
  public static Query q3() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(1L, 1L, 2L, true)); // (b)-->(c)
    tc.add(new Step(2L, 2L, 0L, true)); // (c)-->(a)
    tc.add(new Step(1L, 0L, 0L, true)); // (b)-->(a)
    return new Query(tc, 3, 3);
  }

  /**
   * (a)-->(b)-->(c)-->(a)
   *
   * (0)-0->(1)-1->(2)-2->(0)
   *
   * @return query q4
   */
  public static Query q4() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, true)); // (a)-->(b)
    tc.add(new Step(1L, 1L, 2L, true)); // (b)-->(c)
    tc.add(new Step(2L, 2L, 0L, true)); // (c)-->(a)
    return new Query(tc, 3, 3);
  }

  /**
   * (a)-->(b)-->(a)<--(c)
   *
   * (0)-0->(1)-1->(0)<-2-(2)
   *
   * @return query q5
   */
  public static Query q5() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, true));  // (a)-->(b)
    tc.add(new Step(1L, 1L, 0L, true));  // (b)-->(a)
    tc.add(new Step(0L, 2L, 2L, false)); // (a)<--(c)
    return new Query(tc, 3, 3);
  }

  /**
   * (d)-->(a)-->(b)-->(a)<--(c)
   *
   * (0)-0->(1)-1->(2)-2->(1)<-3-(3)
   *
   * @return query q6
   */
  public static Query q6() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(1L, 1L, 2L, true));  // (a)-->(b)
    tc.add(new Step(2L, 2L, 1L, true));  // (b)-->(a)
    tc.add(new Step(1L, 0L, 0L, false)); // (a)<--(d)
    tc.add(new Step(1L, 3L, 3L, false)); // (a)<--(c)
    return new Query(tc, 4, 4);
  }

  /**
   * (c)<--(a)-->(d)<--(b)-->(c)
   *
   * (0)<-0-(1)-1->(2)<-2-(3)-3->(0)
   *
   * @return query q7
   */
  public static Query q7() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(1L, 0L, 0L, true));  // (a)-->(c)
    tc.add(new Step(1L, 1L, 2L, true));  // (a)-->(d)
    tc.add(new Step(2L, 2L, 3L, false)); // (d)<--(b)
    tc.add(new Step(0L, 3L, 3L, false)); // (c)<--(b)
    return new Query(tc, 4, 4);
  }

  /**
   * (a)<--(b)-->(c)-->(d)
   *
   * (0)<-0-(1)-1->(2)-2->(3)
   *
   * @return query q8
   */
  public static Query q8() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, false)); // (a)<--(b)
    tc.add(new Step(1L, 1L, 2L, true));  // (b)-->(c)
    tc.add(new Step(2L, 2L, 3L, true));  // (c)-->(d)

    return new Query(tc, 4, 3);
  }

  /**
   * (a)<--(b)-->(c)-->(a)<--(d)
   *
   * (0)<-0-(1)-1->(2)-2->(0)<-3-(3)
   *
   * @return query q9
   */
  public static Query q9() {
    TraversalCode tc = new TraversalCode();
    tc.add(new Step(0L, 0L, 1L, false)); // (a)<--(b)
    tc.add(new Step(1L, 1L, 2L, true));  // (b)-->(c)
    tc.add(new Step(2L, 2L, 0L, true));  // (c)-->(a)
    tc.add(new Step(0L, 3L, 3L, false));  // (a)<--(d)

    return new Query(tc, 4, 4);
  }
}
