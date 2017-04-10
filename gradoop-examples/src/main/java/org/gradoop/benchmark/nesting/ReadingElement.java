package org.gradoop.benchmark.nesting;

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by vasistas on 10/04/17.
 */
public class ReadingElement  {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);

    String filePath = System.getProperty("user.home");
    if (!filePath.endsWith(Path.SEPARATOR)) {
      filePath += Path.SEPARATOR;
    }
    filePath += "testing.multifiles";
    List<Long> longList = new ArrayList<>();
    Random r = new Random();
    for (int i=0; i<10; i++) {
      longList.add(r.nextLong());
    }

    env
      .fromCollection(longList)
      .map((Long x)-> x+1L)
      .cross(env.fromCollection(longList))
      .filter((Tuple2<Long, Long> x) -> x.f1 * x.f0 <= 7L && x.f1 > 0 && x.f0 > 0)
      .write(new BinaryOutputFormat<Tuple2<Long, Long>>() {
        byte left[] = new byte[Long.BYTES];
        byte right[] = new byte[Long.BYTES];
        @Override
        protected void serialize(Tuple2<Long, Long> record, DataOutputView dataOutput) throws
          IOException {
          ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
          dataOutput.write(bb.putLong(record.f0).array());
          ByteBuffer bbb = ByteBuffer.allocate(Long.BYTES);
          dataOutput.write(bbb.putLong(record.f0).array());
        }
      }, filePath);
    env.execute();

    env.readFile(new BinaryInputFormat<Long>() {
      byte bytes[] = new byte[Long.BYTES];
      @Override
      protected Long deserialize(Long reuse, DataInputView dataInput) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        byte[] bba = new byte[Long.BYTES];
        dataInput.read(bba);
        bb.put(bba);
        return bb.getLong();
      }
    }, filePath).collect().forEach(System.out::println);
  }

}
