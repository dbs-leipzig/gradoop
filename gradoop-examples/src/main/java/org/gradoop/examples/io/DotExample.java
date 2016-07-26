package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.io.impl.dot.DotDataSink;
import org.gradoop.io.impl.json.JSONDataSource;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Created by galpha on 7/26/16.
 */
public class DotExample extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception{

    if (args.length != 4){
      throw new IllegalArgumentException(
        "provide graph/vertex/edge paths and output directory");
    }

    final String graphHeadFile  = args[0];
    final String vertexFile     = args[1];
    final String edgeFile       = args[2];
    final String outputDir      = args[3];

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create default Gradoop config
    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> config =
      GradoopFlinkConfig.createDefaultConfig(env);

    // create DataSource
    JSONDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new JSONDataSource<>(graphHeadFile, vertexFile, edgeFile, config);

    // read graph collection from DataSource
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> graphCollection =
      dataSource.getGraphCollection();

    DotDataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink = new
      DotDataSink<>(outputDir);

    dataSink.write(graphCollection);

    env.execute();
  }

  @Override
  public String getDescription() {
    return null;
  }
}
