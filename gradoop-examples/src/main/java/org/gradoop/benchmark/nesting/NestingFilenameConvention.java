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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.gradoop.benchmark.nesting.data.Operation;
import org.gradoop.benchmark.nesting.serializers.DeserializeGradoopidFromFile;
import org.gradoop.benchmark.nesting.serializers.DeserializePairOfIdsFromFile;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Created by vasistas on 10/04/17.
 */
public abstract class NestingFilenameConvention extends AbstractRunner {

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
   * Global environment
   */
  protected static ExecutionEnvironment ENVIRONMENT;

  /**
   * GradoopFlink configuration
   */
  protected static GradoopFlinkConfig CONFIGURATION;

  static {
    ENVIRONMENT = getExecutionEnvironment();
    CONFIGURATION = GradoopFlinkConfig.createConfig(ENVIRONMENT);
  }

  /**
   * File where to store the benchmarks
   */
  private final String csvPath;

  /**
   * Bas Path
   */
  private final String basePath;

  /**
   * Mapping each phase (position id) to the execution itself
   */
  private final ArrayList<Operation> phaseMapper;

  /**
   * Mapping each finalization (position id) to the execution itself
   */
  private final ArrayList<Operation> finalizeMapper;

  /**
   * Default constructor for running the tests
   * @param csvPath   File where to store the intermediate results
   * @param basePath  Base path where the indexed data is loaded
   */
  public NestingFilenameConvention(String basePath, String csvPath) {
    this.csvPath = csvPath;
    this.basePath = basePath;
    phaseMapper = new ArrayList<>(2);
    finalizeMapper = new ArrayList<>(2);
  }

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

  /**
   * Loads an index located in a given specific folder + operand prefix
   * @param filename  Foder
   * @return          Loaded index
   */
  public static NestingIndex loadNestingIndex(String filename) {
    DataSet<GradoopId> headers = ENVIRONMENT
      .readFile(new DeserializeGradoopidFromFile(), filename + INDEX_HEADERS_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> vertexIndex = ENVIRONMENT
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_VERTEX_SUFFIX);
    DataSet<Tuple2<GradoopId, GradoopId>> edgeIndex = ENVIRONMENT
      .readFile(new DeserializePairOfIdsFromFile(), filename + INDEX_EDGE_SUFFIX);

    return new NestingIndex(headers, vertexIndex, edgeIndex);
  }

  public String getBasePath() {
    return basePath;
  }

  public void registerNextPhase(Operation phase, Operation finalizer) {
    phaseMapper.add(phase);
    finalizeMapper.add(finalizer);
  }

  public void run() throws Exception {
    for (int phaseNo=0; phaseNo<phaseMapper.size(); phaseNo++) {
      phaseMapper.get(phaseNo).method();
      finalizeMapper.get(phaseNo).method();

      String plan = ENVIRONMENT.getExecutionPlan();


      String ns[] = this.basePath.split("/");
      Files.write(
        Paths.get(Paths.get(this.csvPath).getParent().toString() +
          Path.SEPARATOR + phaseNo + ns[ns.length-1] + ".json"),
        plan.getBytes(Charset.forName("UTF-8")),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);

      // Writing the result of the benchmark to the file
      String line = getClass().getName() + "," +
        "Benchmark," +
        phaseNo + "," +
        this.basePath + "," +
        ENVIRONMENT.execute().getNetRuntime() + "\n";
      Files.write(Paths.get(this.csvPath), line.getBytes(Charset.forName("UTF-8")),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }


}
