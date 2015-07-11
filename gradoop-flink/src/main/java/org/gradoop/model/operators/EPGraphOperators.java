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

package org.gradoop.model.operators;

import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPEdgeCollection;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.EPVertexCollection;

import java.util.List;

/**
 * Describes all operators that can be applied on a single graph inside the
 * EPGM.
 */
public interface EPGraphOperators {

  EPVertexCollection getVertices();

  EPEdgeCollection getEdges();

  EPEdgeCollection getOutgoingEdges(final Long vertexID);

  EPEdgeCollection getIncomingEdges(final Long vertexID);

  long getVertexCount() throws Exception;

  long getEdgeCount() throws Exception;

  /*
  unary operators take one graph as input and return a single graph or a
  graph collection
   */

  EPGraphCollection match(String graphPattern,
    Predicate<EPPatternGraph> predicateFunc);

  EPGraph project(UnaryFunction<EPVertexData, EPVertexData> vertexFunction,
    UnaryFunction<EPEdgeData, EPEdgeData> edgeFunction);

  <O extends Number> EPGraph aggregate(String propertyKey,
    Aggregate<EPGraph, O> aggregateFunc) throws Exception;

  EPGraph summarize(String vertexGroupingKey) throws Exception;

  EPGraph summarize(String vertexGroupingKey, String edgeGroupingKey) throws
    Exception;

  EPGraph summarize(String vertexGroupingKey,
    Aggregate<Iterable<EPVertexData>, Number> vertexAggregateFunc,
    String edgeGroupingKey,
    Aggregate<Iterable<EPEdgeData>, Number> edgeAggregateFunc) throws Exception;

  <O1 extends Number, O2 extends Number> EPGraph summarize(
    List<String> vertexGroupingKeys,
    Aggregate<Iterable<EPVertexData>, O1> vertexAggregateFunc,
    List<String> edgeGroupingKeys,
    Aggregate<Iterable<EPEdgeData>, O2> edgeAggregateFunc) throws Exception;

  /*
  binary operators take two graphs as input and return a single graph
   */

  EPGraph combine(EPGraph otherGraph);

  EPGraph overlap(EPGraph otherGraph);

  EPGraph exclude(EPGraph otherGraph);

  /*
  auxiliary operators
   */

  EPGraph callForGraph(Algorithm algorithm, String... params);

  EPGraphCollection callForCollection(Algorithm algorithm, String... params);
}
