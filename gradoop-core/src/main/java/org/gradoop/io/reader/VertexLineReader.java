///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.io.reader;
//
//import org.gradoop.model.VertexData;
//
//import java.util.List;
//
///**
// * Used to read a vertex from an input string. Used e.g. in
// * {@link org.gradoop.io.reader.AdjacencyListReader} and
// * {@link org.gradoop.io.reader.BulkLoadEPG}.
// */
//public interface VertexLineReader {
//  /**
//   * Parses a given line and creates a vertex instance for further processing.
//   *
//   * @param line string encoded vertex
//   * @return vertex instance
//   */
//  VertexData readVertex(final String line);
//
//  /**
//   * Parses a given line and creates one or multiple vertex instances for
//   * further processing.
//   * <p/>
//   * This method return {@code null} if {@code supportsVertexLists()} returns
//   * false.
//   *
//   * @param line string encoded vertices
//   * @return vertex list or {@code null} if lists are not supported
//   */
//  List<VertexData> readVertexList(final String line);
//
//  /**
//   * True, if the reader supports reading multiple vertices from a single
//   * input line, which can be necessary in some formats.
//   *
//   * @return true if reader has list support, false otherwise
//   */
//  boolean supportsVertexLists();
//}
