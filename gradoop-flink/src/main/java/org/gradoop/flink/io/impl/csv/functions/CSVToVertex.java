package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;


public class CSVToVertex
  implements MapFunction<Tuple2<LongWritable, Text>, Vertex>{

  private VertexFactory vertexFactory;

  public CSVToVertex(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public Vertex map(Tuple2<LongWritable, Text> longWritableTuple2) throws
    Exception {
    return vertexFactory.createVertex();
  }
}
