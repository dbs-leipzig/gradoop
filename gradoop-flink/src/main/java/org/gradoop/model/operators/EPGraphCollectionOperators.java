/*
 * This file is part of Gradoop.
 *
 *     Gradoop is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Foobar is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.operators;

import org.gradoop.model.EPGraphData;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;

import java.util.List;

/**
 * Defines operators that can be called on a collection of logical graphs.
 *
 * @author Martin Junghanns
 */
public interface EPGraphCollectionOperators<T> extends
  EPCollectionOperators<T> {

  /**
   * Get graph from collection by identifier.
   *
   * @param graphID graph identifier
   * @return logical graph with given id
   * @throws Exception
   */
  EPGraph getGraph(final Long graphID) throws Exception;

  EPGraphCollection getGraphs(final Long... identifiers) throws Exception;

  EPGraphCollection getGraphs(List<Long> identifiers) throws Exception;

  long getGraphCount() throws Exception;

  /**
   * Filter containing graphs based on their properties.
   *
   * @param predicateFunction predicate function for graph data
   * @return collection with logical graphs that fulfil the predicate
   * @throws Exception
   */
  EPGraphCollection filter(Predicate<EPGraphData> predicateFunction) throws
    Exception;

  /*
  collection operators
   */

  EPGraphCollection select(Predicate<EPGraph> predicateFunction) throws
    Exception;

  EPGraphCollection union(EPGraphCollection otherCollection) throws Exception;

  EPGraphCollection intersect(EPGraphCollection otherCollection) throws
    Exception;

  EPGraphCollection intersectWithSmall(EPGraphCollection otherCollection) throws
    Exception;

  EPGraphCollection difference(EPGraphCollection otherCollection) throws
    Exception;

  EPGraphCollection differenceWithSmallResult(
    EPGraphCollection otherCollection) throws Exception;

  EPGraphCollection distinct();

  EPGraphCollection sortBy(String propertyKey, Order order);

  EPGraphCollection top(int limit);

  /*
  auxiliary operators
   */

  EPGraphCollection apply(UnaryGraphToGraphOperator op);

  EPGraph reduce(BinaryGraphToGraphOperator op);

  EPGraphCollection callForCollection(UnaryCollectionToCollectionOperator op);

  EPGraphCollection callForCollection(BinaryCollectionToCollectionOperator op,
    EPGraphCollection otherCollection) throws Exception;

  EPGraph callForGraph(UnaryCollectionToGraphOperator op);


}
