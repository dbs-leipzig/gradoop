/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
