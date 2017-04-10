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
