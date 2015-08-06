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
//package org.gradoop.io.writer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.gradoop.GConstants;
//import org.gradoop.storage.hbase.VertexHandler;
//
//import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;
//
///**
// * Bulk Export from Gradoop into map output files. Reads data from the graph
// * repository and writes them into local files using a user-defined
// * {@link org.gradoop.io.writer.VertexLineWriter}
// */
//public class BulkWriteEPG extends TableMapper<Text, NullWritable> {
//
//  /**
//   * Configuration key for vertex line writer.
//   */
//  public static final String VERTEX_LINE_WRITER =
//    BulkWriteEPG.class.getName() + ".vertex_line_writer";
//
//  /**
//   * Configuration key for the vertex handler.
//   */
//  public static final String VERTEX_HANDLER =
//    BulkWriteEPG.class.getName() + ".vertex_handler";
//
//  /**
//   * Used for the map output value.
//   */
//  private static NullWritable NULL_OUTPUT = NullWritable.get();
//
//  /**
//   * Used to read vertices from the repository.
//   */
//  private VertexHandler vertexHandler;
//
//  /**
//   * Used to convert vertices into strings.
//   */
//  private VertexLineWriter vertexLineWriter;
//
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  protected void setup(Context context) throws IOException,
//    InterruptedException {
//    Configuration conf = context.getConfiguration();
//
//    Class<? extends VertexHandler> handlerClass = conf
//      .getClass(GConstants.VERTEX_HANDLER_CLASS,
//        GConstants.DEFAULT_VERTEX_HANDLER, VertexHandler.class);
//
//    Class<? extends VertexLineWriter> writerClass =
//      conf.getClass(VERTEX_LINE_WRITER, null, VertexLineWriter.class);
//
//    try {
//      this.vertexHandler = handlerClass.getConstructor().newInstance();
//      this.vertexLineWriter = writerClass.getConstructor().newInstance();
//    } catch (InvocationTargetException | NoSuchMethodException |
//      IllegalAccessException | InstantiationException e) {
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   * Converts the row into a vertex, uses the vertex line writer to create a
//   * string representation and creates a map output based on that.
//   *
//   * @param key     row key
//   * @param value   hbase row result for a single vertex.
//   * @param context task context
//   * @throws IOException
//   * @throws InterruptedException
//   */
//  @Override
//  protected void map(ImmutableBytesWritable key, Result value,
//    Context context) throws IOException, InterruptedException {
//    context.write(new Text(
//        this.vertexLineWriter.writeVertex(vertexHandler.readVertex(value))),
//      NULL_OUTPUT);
//  }
//}
