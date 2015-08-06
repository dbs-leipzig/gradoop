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
//import com.google.common.collect.Lists;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.VertexData;
//import org.gradoop.model.impl.EdgeFactory;
//import org.gradoop.model.impl.VertexFactory;
//
//import java.util.List;
//import java.util.regex.Pattern;
//
///**
// * Reader for simple adjacency list.
// * <p/>
//// * vertex-id neighbour1-id neighbour2-id ...
// */
//public class SimpleVertexReader extends SingleVertexReader {
//  /**
//   * Separates a line into tokens.
//   */
//  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");
//
//  /**
//   * Separates the whole line using {@code LINE_TOKEN_SEPARATOR}.
//   *
//   * @param line single input line
//   * @return token array
//   */
//  private String[] getTokens(String line) {
//    return LINE_TOKEN_SEPARATOR.split(line);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public VertexData readVertex(String line) {
//    String[] tokens = getTokens(line);
//    Long vertexID = Long.valueOf(tokens[0]);
//
//    if (tokens.length > 1) {
//      List<EdgeData> edgeDatas = Lists.newArrayListWithCapacity(tokens.length - 1);
//      for (int i = 1; i < tokens.length; i++) {
//        Long otherID = Long.valueOf(tokens[i]);
//        EdgeData e = EdgeFactory.createDefaultEdge(otherID, (long) i - 1);
//        edgeDatas.add(e);
//      }
//      return
//        VertexFactory.createDefaultVertexWithOutgoingEdges(vertexID, edgeDatas);
//    } else {
//      return VertexFactory.createDefaultVertexWithID(vertexID);
//    }
//  }
//}
