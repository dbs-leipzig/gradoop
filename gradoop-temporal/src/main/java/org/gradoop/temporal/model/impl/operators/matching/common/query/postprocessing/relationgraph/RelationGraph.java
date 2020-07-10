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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.relationgraph;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.JohnsonSimpleCycles;
import org.jgrapht.graph.GraphWalk;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.NEQ;

/**
 * Represents a set of comparisons as a graph. Each comparison {@code lhs comparator rhs} is represented
 * as an edge {@code (lhs)-[comparator]->(rhs)}.
 * If the comparator is symmetrical (= or !=), there is also an edge {@code (rhs)-[comparator]-(lhs)}
 * The sole purpose of this class is finding all circles in this graph. Such a circle may imply a
 * contradictory query.
 * <p>
 * !!! This class assumes the comparisons to be normalize, i.e. not containing < or <= !!!
 */
public class RelationGraph {

  /**
   * The relations graph
   */
  Graph<ComparableExpression, ComparatorEdge> relationsGraph;

  /**
   * Creates a new instance
   *
   * @param comparisons set of comparisons to build the graph from.
   *                    It is assumed that they are normalized, i.e. no comparison
   *                    with comparator < or <= is contained
   */
  public RelationGraph(Set<ComparisonExpressionTPGM> comparisons) {
    this.relationsGraph = new SimpleDirectedGraph<>(ComparatorEdge.class);

    for (ComparisonExpressionTPGM comparison : comparisons) {
      ComparableExpression lhs = comparison.getLhs().getWrappedComparable();
      ComparableExpression rhs = comparison.getRhs().getWrappedComparable();
      Comparator comparator = comparison.getComparator();

      relationsGraph.addVertex(lhs);
      relationsGraph.addVertex(rhs);
      if (!lhs.equals(rhs)) {
        relationsGraph.addEdge(lhs, rhs, new ComparatorEdge(comparator));
      }


      // symmetric relations
      if (comparator == EQ || comparator == NEQ) {
        if (!lhs.equals(rhs)) {
          relationsGraph.addEdge(rhs, lhs, new ComparatorEdge(comparator));
        }
      }
    }
  }

  /**
   * Finds all circles in the relations graph
   *
   * @return list of list of comparators. Every list represents a cyclic path in the
   * relations graph by the path's edges (= comparators)
   */
  public List<List<Comparator>> findAllCircles() {
    // find cyclic paths as enumerations of nodes (=ComparableExpression s)
    List<List<ComparableExpression>> pathsAsNodes =
      new JohnsonSimpleCycles<ComparableExpression, ComparatorEdge>(relationsGraph)
        .findSimpleCycles()
        .stream()
        // findSimpleCycles() does not automatically append the start vertex to the path
        .peek(nodeList ->
          nodeList.add(nodeList.get(0)))
        .collect(Collectors.toList());

    // convert every path to an enumerations of its edges (=comparators)
    List<List<Comparator>> pathsAsComparators = new ArrayList<>();
    for (List<ComparableExpression> pathAsNodes : pathsAsNodes) {
      pathsAsComparators.add(
        // "walk" the path and collect the comparators on the way
        new GraphWalk<ComparableExpression, ComparatorEdge>(
          relationsGraph, pathAsNodes, 1.)
          .getEdgeList().stream()
          .map(edge -> edge.comp)
          .collect(Collectors.toList())
      );
    }
    return pathsAsComparators;
  }

  /**
   * Class that wraps a Comparator. Needed because the {@link SimpleDirectedGraph} does not
   * allow a single object to represent more than one edge
   */
  static class ComparatorEdge {

    /**
     * wrapped comparator
     */
    Comparator comp;

    /**
     * Creates a new instance
     * @param comp comparator
     */
    protected ComparatorEdge(Comparator comp) {
      this.comp = comp;
    }

    public Comparator getComp() {
      return comp;
    }

    public void setComp(Comparator comp) {
      this.comp = comp;
    }
  }
}
