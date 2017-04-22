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

package org.gradoop.examples.io.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.examples.io.parsers.functions.FromAdjacencyListableToVertex;
import org.gradoop.examples.io.parsers.functions.FromStringToAdjacencyList;
import org.gradoop.examples.io.parsers.functions.ToEdgesFromAdjList;
import org.gradoop.examples.io.parsers.inputfilerepresentations.AdjacencyListable;
import org.gradoop.examples.io.parsers.inputfilerepresentations.Edgable;
import org.gradoop.examples.io.parsers.rawedges.FileReaderForParser;


/**
 * General purpose class for reading data represented as an adjacency list
 *
 * @param <Element>   The intermediate representation of the parsed string
 * @param <Edge>      The class representing an edge from Element
 * @param <Adj>       The class representing the Adjacency list, that is the source vertex with all
 *                    the outgoing edges
 */
public class AdjacencyListFileParser<Element extends Comparable<Element>,
                          Edge extends Edgable<Element>,
                          Adj extends AdjacencyListable<Element, Edge>>
                          extends FileReaderForParser {



  /**
   * Transformation from a String into an intermediate Adjacency List (Adj)
   */
  private final FromStringToAdjacencyList<Element, Edge, Adj> fromStringToAdjConcrete;

  /**
   * Transformation of the adjacency list into a vertex
   */
  private final FromAdjacencyListableToVertex<Element, Edge, Adj> fromElementToVertexConcrete;

  /**
   * Transformation of the adjacency list into vertices
   */
  private final ToEdgesFromAdjList<Element, Edge, Adj> fromElementToAdjConcrete;


  /**
   * Default constructor
   * @param fromStringToAdjConcrete     Transformation from a String into an intermediate
   *                                    Adjacency List (Adj)
   * @param fromElementToVertexConcrete Transformation of the adjacency list into a vertex
   * @param fromElementToAdjConcrete    Transformation of the adjacency list into vertices
   */
  public AdjacencyListFileParser(
    FromStringToAdjacencyList<Element, Edge, Adj> fromStringToAdjConcrete,
    FromAdjacencyListableToVertex<Element, Edge, Adj> fromElementToVertexConcrete,
    ToEdgesFromAdjList<Element, Edge, Adj> fromElementToAdjConcrete) {
    super("\n");
    this.fromStringToAdjConcrete = fromStringToAdjConcrete;
    this.fromElementToVertexConcrete = fromElementToVertexConcrete;
    this.fromElementToAdjConcrete = fromElementToAdjConcrete;
  }


  /**
   * Specifies the file delimiter containing the String
   * @param delimiter  string delimiter
   * @return      Updated instance of this
   */
  public AdjacencyListFileParser splitWith(String delimiter) {
    super.setDelimiter(delimiter);
    return this;
  }

  /**
   * Function to be used when all the default parameters are null
   * @param concrete    Mapping function for each record in the dataset
   * @param <T>         Target type
   * @return            Dataset of <T>
   */
  public <T> DataSet<T> getDataset(MapFunction<String, T> concrete) {
    return readAsStringDataSource().map(concrete);
  }

  /**
   * Converts the element into a graph clob
   * @return    General datasource that has already to be processed with other files
   */
  public GraphClob<Element> asGeneralGraphDataSource() {
    DataSet<Adj> tmpVertices = readAsStringDataSource().map(fromStringToAdjConcrete);
    DataSet<ImportVertex<Element>> vertices = tmpVertices.map(fromElementToVertexConcrete);
    DataSet<ImportEdge<Element>> edges = tmpVertices.flatMap(fromElementToAdjConcrete);
    return new GraphClob<Element>(vertices, edges);
  }

}
