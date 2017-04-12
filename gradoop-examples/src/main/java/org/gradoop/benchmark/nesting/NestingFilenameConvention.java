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
package org.gradoop.benchmark.nesting;

import org.apache.flink.core.fs.Path;
import org.gradoop.examples.AbstractRunner;

/**
 * Created by vasistas on 10/04/17.
 */
public class NestingFilenameConvention extends AbstractRunner {

  /**
   * Represents the path suffix describing the files for the headers
   */
  protected static final String INDEX_HEADERS_SUFFIX = "-heads.bin";

  /**
   * Represents the path suffix describing the files for the vertices
   */
  protected static final String INDEX_VERTEX_SUFFIX = "-vertex.bin";

  /**
   * Represents the path suffix describing the edges
   */
  protected static final String INDEX_EDGE_SUFFIX = "-edges.bin";

  /**
   * Represents the file prefix for the files describing pieces of information for the
   * left operand
   */
  protected static final String LEFT_OPERAND = "left";

  /**
   * Represents the file prefix for the files describing pieces of informations for the
   * right operand
   */
  protected static final String RIGHT_OPERAND = "right";

  /**
   * Generating the base path for the strings
   * @param path            Base path
   * @param isLeftOperand   Checks if it is a left operand
   * @return                Initialized and finalized string
   */
  public static String generateOperandBasePath(String path, boolean isLeftOperand) {
    return path +
            (path.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) +
            (isLeftOperand ? LEFT_OPERAND : RIGHT_OPERAND);
  }


}
