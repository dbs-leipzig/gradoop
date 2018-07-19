/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.examples.io;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Calendar;
import java.util.regex.Matcher;
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
  private static final String PROFILES = "/soc-pokec-profiles.txt";
  /**
   * Filename that contains Pokec relationships
   */
  private static final String RELATIONSHIPS = "/soc-pokec-relationships.txt";
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
   * Position of the "gender" attribute in the profiles CSV
   */
  private static final int CSV_IDX_GENDER = 3;
  /**
   * Position of the "region" attribute in the profiles CSV
   */
  private static final int CSV_IDX_REGION = 4;
  /**
   * Position of the "age" attribute in the profiles CSV
   */
  private static final int CSV_IDX_AGE = 7;
  /**
   * Position of the "body" attribute in the profile CSV
   */
  private static final int CSV_IDX_BODY = 8;
  /**
   * Position of the "I_am_working_in_field" attribute in the profile CSV
   */
  private static final int CSV_WORKING_FIELD = 9;
  /**
   * Position of the "eye_color" attribute in the profile CSV
   */
  private static final int CSV_EYE_COLOR = 16;
  /**
   * Position of the "hair_color" attribute in the profile CSV
   */
  private static final int CSV_HAIR_COLOR = 17;
  /**
   * Property key to use for the gender attribute
   */
  private static final String PROP_KEY_GENDER = "gender";
  /**
   * Property key to use for the region attribute
   */
  private static final String PROP_KEY_REGION = "region";
  /**
   * Property key to use for the decade attribute
   */
  private static final String PROP_KEY_DECADE = "decade";
  /**
   * Property key to use for the height attribute
   */
  private static final String PROP_KEY_HEIGHT = "height";
  /**
   * Property key to use for the height group attribute
   */
  private static final String PROP_KEY_HEIGHT_GROUP = "height_group";
  /**
   * Property key to use for the weight attribute
   */
  private static final String PROP_KEY_WEIGHT = "weight";
  /**
   * Property key to use for the weight group attribute
   */
  private static final String PROP_KEY_WEIGHT_GROUP = "weight_group";
  /**
   * Property key to use for the working field attribute
   */
  private static final String PROP_KEY_WORKING_FIELD = "working_field";
  /**
   * Property key to use for the eye color attribute
   */
  private static final String PROP_KEY_EYE_COLOR = "eye_color";
  /**
   * Property key to use for the hair color attribute
   */
  private static final String PROP_KEY_HAIR_COLOR = "hair_color";

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
        private Pattern splitPattern = Pattern.compile("\\t");

        private Pattern numberPattern = Pattern.compile("(\\d+)");

        private int year = Calendar.getInstance().get(Calendar.YEAR);

        private ImportVertex<Long> importVertex = new ImportVertex<>();

        @SuppressWarnings("Duplicates")
        @Override
        public ImportVertex<Long> map(String line) throws Exception {
          String[] fields = line.split(splitPattern.pattern());

          importVertex.setId(Long.parseLong(fields[0])); // user-id
          importVertex.setLabel(VERTEX_LABEL);

          Properties properties = Properties.create();

          // set gender if existing
          String field = fields[CSV_IDX_GENDER];
          if (!field.equals(NULL_STRING)) {
            int gender = Integer.parseInt(field);
            properties.set(PROP_KEY_GENDER, gender == 1 ? "male" : "female");
          }
          // set region if existing
          if (!fields[CSV_IDX_REGION].equals(NULL_STRING)) {
            properties.set(PROP_KEY_REGION, fields[CSV_IDX_REGION]);
          }
          // compute year of birth from users age (if existing)
          field = fields[CSV_IDX_AGE];
          if (!field.equals(NULL_STRING) && !field.equals("0")) {
            int yob = year - Integer.parseInt(field);
            properties.set(PROP_KEY_DECADE, yob - yob % 10);
          }
          // try to get the height and the weight of the user
          field = fields[CSV_IDX_BODY];
          if (!field.equals(NULL_STRING)) {
            Matcher matcher = numberPattern.matcher(field);

            if (matcher.find()) {
              try {
                int height = Integer.parseInt(matcher.group(1));
                int heightGroup = height - height % 10;
                properties.set(PROP_KEY_HEIGHT, height);
                properties.set(PROP_KEY_HEIGHT_GROUP, heightGroup);
              } catch (NumberFormatException ignored) { }
            }
            if (matcher.find()) {
              try {
                int weight = Integer.parseInt(matcher.group(1));
                int weightGroup = weight - weight % 10;
                properties.set(PROP_KEY_WEIGHT, weight);
                properties.set(PROP_KEY_WEIGHT_GROUP, weightGroup);
              } catch (NumberFormatException ignored) { }
            }
          }
          // set working field if existing
          field = fields[CSV_WORKING_FIELD];
          if (!field.equals(NULL_STRING) && !field.contains(CSVConstants.VALUE_DELIMITER)) {
            properties.set(PROP_KEY_WORKING_FIELD, fields[CSV_WORKING_FIELD]);
          }
          // set eye color if existing
          field = fields[CSV_EYE_COLOR];
          if (!field.equals(NULL_STRING) && !field.contains(CSVConstants.VALUE_DELIMITER)) {
            properties.set(PROP_KEY_EYE_COLOR, fields[CSV_EYE_COLOR]);
          }
          // set hair color if existing
          field = fields[CSV_HAIR_COLOR];
          if (!field.equals(NULL_STRING) && !field.contains(CSVConstants.VALUE_DELIMITER)) {
            properties.set(PROP_KEY_HAIR_COLOR, fields[CSV_HAIR_COLOR]);
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
          importEdge.setProperties(Properties.create());
          return importEdge;
        }
      }).withForwardedFields("f0;f1.f0->f1;f1.f1->f2");

    //--------------------------------------------------------------------------
    // Example Graph Analytics
    //--------------------------------------------------------------------------

    // Create an EPGM logical graph
    new GraphDataSource<>(importVertices, importEdges, config)
      .getLogicalGraph()
      // store the result into csv files
      .writeTo(new CSVDataSink(
        outputDir,
        config
      ));

    env.execute();
  }
}
