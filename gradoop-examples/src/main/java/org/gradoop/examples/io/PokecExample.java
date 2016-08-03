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

package org.gradoop.examples.io;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Example program that reads the Pokec social network from a CSV representation
 * into a {@link LogicalGraph}, build a summary graph based on users attributed
 * and stores it into JSON files.
 *
 * The dataset is available under https://snap.stanford.edu/data/soc-pokec.html.
 */
public class PokecExample {
  /**
   * Filename that contains Pokec profiles
   */
  private static final String PROFILES = "soc-pokec-profiles.txt";
  /**
   * Filename that contains Pokec relationships
   */
  private static final String RELATIONSHIPS = "soc-pokec-relationships.txt";
  /**
   * In the dataset, a missing value is denoted by that value.
   */
  private static final String NULL_STRING = "null";
  /**
   * Vertex label to use during import
   */
  private static final String VERTEX_LABEL = "Person";
  /**
   * Edge label to use during import
   */
  private static final String EDGE_LABEL = "knows";
  /**
   * Position of the gender attribute in the profiles CSV
   */
  private static final int CSV_IDX_GENDER = 3;
  /**
   * Position of the region attribute in the profiles CSV
   */
  private static final int CSV_IDX_REGION = 4;
  /**
   * Position of the age attribute in the profiles CSV
   */
  private static final int CSV_IDX_AGE = 7;
  /**
   * Property key to use for the gender attribute
   */
  private static final String PROP_KEY_GENDER = "g";
  /**
   * Property key to use for the region attribute
   */
  private static final String PROP_KEY_REGION = "c";
  /**
   * Property key to use for the decade attribute
   */
  private static final String PROP_KEY_DECADE = "d";

  /**
   * Reads the Pokec network from a given directory. The graph can be stored in
   * local file system or HDFS.
   *
   * args[0]: path to directory that contains pokec files (e.g. hdfs:///pokec/)
   * args[1]: path to write the output graph to (e.g. hdfs:///output/)
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final String inputDir = args[0];
    final String outputDir = args[1];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop config
    final GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    final String profiles = inputDir + PROFILES;
    final String relations = inputDir + RELATIONSHIPS;

    //--------------------------------------------------------------------------
    // Read vertices
    //--------------------------------------------------------------------------

    // read profiles from file and create import vertices
    DataSet<ImportVertex<Long>> importVertices = env
      .readTextFile(profiles)
      .map(new MapFunction<String, ImportVertex<Long>>() {
        private Pattern p = Pattern.compile("\\t");

        private int year = Calendar.getInstance().get(Calendar.YEAR);

        private ImportVertex<Long> importVertex = new ImportVertex<>();

        @Override
        public ImportVertex<Long> map(String line) throws Exception {
          String[] fields = line.split(p.pattern());

          importVertex.setId(Long.parseLong(fields[0])); // user-id
          importVertex.setLabel(VERTEX_LABEL);

          PropertyList properties = PropertyList.create();

          // set gender if existing
          String genderString = fields[CSV_IDX_GENDER];
          if (!genderString.equals(NULL_STRING)) {
            properties.set(PROP_KEY_GENDER, Integer.parseInt(genderString));
          }
          // set region if existing
          if (!fields[CSV_IDX_REGION].equals(NULL_STRING)) {
            properties.set(PROP_KEY_REGION, fields[4]);
          }
          // compute year of birth from users age (if existing)
          String ageString = fields[CSV_IDX_AGE];
          if (!ageString.equals(NULL_STRING) && !ageString.equals("0")) {
            int yob = year - Integer.parseInt(ageString);
            properties.set(PROP_KEY_DECADE, yob - yob % 10);
          }
          importVertex.setProperties(properties);

          return importVertex;
        }
      });

    //--------------------------------------------------------------------------
    // Read edges
    //--------------------------------------------------------------------------

    // read relationships from file (edges)
    // each edge is represented by a Tuple2 (source-id, target-id)
    DataSource<Tuple2<Long, Long>> edges = env
      .readCsvFile(relations)
      .fieldDelimiter("\t")
      .includeFields(true, true)
      .types(Long.class, Long.class);

    // assign a unique long id to each edge tuple
    DataSet<Tuple2<Long, Tuple2<Long, Long>>> edgesWithId =
      DataSetUtils.zipWithUniqueId(edges);

    // transform to ImportEdge
    final DataSet<ImportEdge<Long>> importEdges = edgesWithId.map(
      new MapFunction<Tuple2<Long, Tuple2<Long, Long>>, ImportEdge<Long>>() {

        private final ImportEdge<Long> importEdge = new ImportEdge<>();

        @Override
        public ImportEdge<Long> map(
          Tuple2<Long, Tuple2<Long, Long>> value) throws Exception {
          importEdge.setId(value.f0);
          importEdge.setSourceId(value.f1.f0);
          importEdge.setTargetId(value.f1.f1);
          importEdge.setLabel(EDGE_LABEL);
          importEdge.setProperties(PropertyList.create());
          return importEdge;
        }
      }).withForwardedFields("f0;f1.f0->f1;f1.f1->f2");

    //--------------------------------------------------------------------------
    // Example Graph Analytics
    //--------------------------------------------------------------------------

    // Create an EPGM logical graph
    new GraphDataSource<>(importVertices, importEdges, config)
      .getLogicalGraph()
      // group the graph by users region and the decade they were born in
      .groupBy(Lists.newArrayList(PROP_KEY_REGION, PROP_KEY_DECADE))
      // store the result into json files
      .writeTo(new JSONDataSink(
        outputDir + "graphHeads.json",
        outputDir + "vertices.json",
        outputDir + "edges.json",
        config
      ));

    env.execute();
  }
}
