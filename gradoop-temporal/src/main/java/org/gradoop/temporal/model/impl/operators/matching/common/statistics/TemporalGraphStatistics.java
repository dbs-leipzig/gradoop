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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.Optional;

/**
 * Abstract base class for statistics about temporal graphs
 */
public abstract class TemporalGraphStatistics {

  /**
   * Returns the factory to create instances.
   *
   * @return factory
   */
  public abstract TemporalGraphStatisticsFactory getFactory();

  /**
   * Returns the number of vertices with a given label
   *
   * @param label label of the vertices to count
   * @return number of vertices with a given label
   */
  public abstract long getVertexCount(String label);

  /**
   * Returns total number of vertices
   *
   * @return total number of vertices
   */
  public abstract long getVertexCount();

  /**
   * Returns the number of edges with a given label
   *
   * @param label label of the edges to count
   * @return number of edges with a given label
   */
  public abstract long getEdgeCount(String label);

  /**
   * Returns total number of edges
   *
   * @return total number of edges
   */
  public abstract long getEdgeCount();

  /**
   * Counts or estimates the number of distinct source vertices for edges
   * with a certain label
   *
   * @param edgeLabel label of the edge to count the vertices
   * @return number of distinct source vertices for edges with given label
   */
  public abstract long getDistinctSourceVertexCount(String edgeLabel);

  /**
   * Counts or estimates the number of distinct source vertices
   *
   * @return number of distinct source vertices
   */
  public abstract long getDistinctSourceVertexCount();

  /**
   * Counts or estimates the number of distinct target vertices
   *
   * @return number of distinct target vertices
   */
  public abstract long getDistinctTargetVertexCount();

  /**
   * Counts or estimates the number of distinct target vertices for edges
   * with a certain label
   *
   * @param edgeLabel label of the edge to count the target vertices
   * @return number of distinct target vertices for edges with given label
   */
  public abstract long getDistinctTargetVertexCount(String edgeLabel);

  /**
   * Estimates the probability that a temporal property (tx_from, tx_to, valid_from, valid_to)
   * of an element with a certain label satisfies a constraint comparing it to a literal
   *
   * @param type1  the type of the element to compare (vertex or edge)
   * @param label1 label of the element to compare (if known)
   * @param field1 time field (tx_from, tx_to, valid_from, valid_to) of the element to compare
   * @param comp   comparator
   * @param value  long constant (i.e. literal) to compare the element with
   * @return estimation of the probability that the condition holds
   */
  public abstract double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                              TimeSelector.TimeField field1,
                                              Comparator comp, Long value);

  /**
   * Estimates the probability that a comparison between two time selectors
   * holds. The return value may only be reasonable, if the time selectors
   * refer to distinct variables.
   *
   * @param type1  the type of the lhs element to compare (vertex or edge)
   * @param label1 label of the lhs element to compare (if known)
   * @param field1 time property (tx_from, tx_to, valid_from, valid_to) of the lhs
   *               element to compare
   * @param comp   comparator
   * @param type2  the type of the rhs element to compare (vertex or edge)
   * @param label2 label of the rhs element to compare (if known)
   * @param field2 time property (tx_from, tx_to, val_from, val_to) of the rhs
   *               element to compare
   * @return estimation of the probability that the condition holds
   */
  public abstract double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                              TimeSelector.TimeField field1,
                                              Comparator comp,
                                              ElementType type2, Optional<String> label2,
                                              TimeSelector.TimeField field2);

  /**
   * Estimates the probability that a comparison of the form
   * {@code variable.interval comparator constant}
   * holds.
   *
   * @param type        type of the lhs element (vertex/edge)
   * @param label       label of the lhs element
   * @param comp        comparator of the comparison
   * @param transaction indicates whether transaction time should be compared (=>true)
   *                    or valid time (=> false)
   * @param value       lhs duration constant
   * @return estimated probability that the comparison holds
   */
  public abstract double estimateDurationProb(ElementType type, Optional<String> label,
                                              Comparator comp, boolean transaction, Long value);

  /**
   * Estimates the probability that a comparison of the form
   * {@code variable.duration comparator variable.duration}
   * holds.
   *
   * @param type1        type of the lhs element (vertex/edge)
   * @param label1       label of the lhs element
   * @param transaction1 indicates whether lhs transaction time should be compared (=>true)
   *                     or valid time (=> false)
   * @param comp comparator of the comparison
   * @param type2 type of the rhs element (vertex/edge)
   * @param label2 label of the rhs element
   * @param transaction2 indicates whether rhs transaction time should be compared (=>true) or valid time
   *                     (=> false)
   * @return estimated probability that the comparison holds
   */
  public abstract double estimateDurationProb(ElementType type1, Optional<String> label1,
                                              boolean transaction1, Comparator comp,
                                              ElementType type2, Optional<String> label2,
                                              boolean transaction2);

  /**
   * Estimates the probability that a comparison of a property value with a constant holds
   *
   * @param type  type of the lhs (vertex/edge)
   * @param label label of the lhs, if known
   * @param property property to compare
   * @param comp  comparator of the comparison
   * @param value rhs value
   * @return estimated probability that the comparison holds
   */
  public abstract double estimatePropertyProb(ElementType type, Optional<String> label, String property,
                                              Comparator comp, PropertyValue value);

  /**
   * Estimates the probability that a comparison between two property selectors holds
   *
   * @param type1     type of the lhs element (vertex/edge)
   * @param label1    label of the lhs element, if known
   * @param property1 lhs property key
   * @param comp      comparator of the comparison
   * @param type2     type of the rhs element (vertex/edge)
   * @param label2    label of the rhs element, if known
   * @param property2 rhs property key
   * @return estimated probability that the comparison holds
   */
  public abstract double estimatePropertyProb(ElementType type1, Optional<String> label1,
                                              String property1, Comparator comp,
                                              ElementType type2, Optional<String> label2,
                                              String property2);

  /**
   * Estimates the probability that a vertex or edge has a certain label
   * @param type type of the element in question (vertex or edge)
   * @param label label in question
   * @return estimated probability that the given type has the given label
   */
  //public abstract double estimateLabelProb(ElementType type, String label);

  /**
   * Describes the two types of graph elements
   */
  public enum ElementType { VERTEX, EDGE }


}
