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

  protected final static String INDEX_HEADERS_SUFFIX = "-heads.bin";

  protected final static String INDEX_VERTEX_SUFFIX = "-vertex.bin";

  protected final static String INDEX_EDGE_SUFFIX = "-edges.bin";

  protected final static String LEFT_OPERAND = "left";

  protected final static String RIGHT_OPERAND = "right";

  public static String generateOperandBasePath(String path, boolean isLeftOperand) {
    return path +
            (path.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) +
            (isLeftOperand ? LEFT_OPERAND : RIGHT_OPERAND);
  }


}
