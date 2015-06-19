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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model;

import javafx.util.Pair;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;

import java.util.List;
import java.util.Set;

public interface EPGraph extends Identifiable, Attributed, Labeled {

  /*
  CRUD operators
   */

  EPVertexSet getVertices();

  EPEdgeSet getEdges();

  /*
  unary operators
   */

  EPGraphCollection match(String graphPattern,
    Predicate<EPPatternGraph> predicateFunc);

  EPGraph project(UnaryFunction<Vertex, Vertex> vertexFunction,
    UnaryFunction<Edge, Edge> edgeFunction);

  <O extends Number> EPGraph aggregate(String propertyKey,
    Aggregate<EPGraph, O> aggregateFunc);

  EPGraph summarize(List<String> vertexGroupingKeys,
    Aggregate<Pair<Vertex, Set<Vertex>>, Vertex> vertexAggregateFunc,
    List<String> edgeGroupingKeys,
    Aggregate<Pair<Edge, Set<Edge>>, Edge> edgeAggregateFunc);

  /*
  binary operators
   */

  EPGraph combine(EPGraph otherGraph);

  EPGraph overlap(EPGraph otherGraph);

  EPGraph exclude(EPGraph otherGraph);

  /*
  auxiliary operators
   */

  EPGraph callForGraph(Algorithm algorithm, String... params);

  EPGraphCollection callForCollection(Algorithm algorithm, String... params);

  long getVertexCount();

  long getEdgeCount();
}
