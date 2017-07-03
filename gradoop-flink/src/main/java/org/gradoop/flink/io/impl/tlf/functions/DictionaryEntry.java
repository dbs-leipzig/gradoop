
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * After the TLF dictionary file has been read with a normal text input
 * format its result text has to be split and formed into Tuple2<Integer,
 * String>.
 */
public class DictionaryEntry implements
  MapFunction<Tuple2<LongWritable, Text>, Tuple2<Integer, String>> {

  /**
   * Tuple which is used as return variable for the mapping
   */
  private Tuple2<Integer, String> returnTuple;

  /**
   * Empty constructor which initializes the return tuple.
   */
  public DictionaryEntry() {
    returnTuple = new Tuple2<>();
  }

  /**
   * Creates a tuple of integer and string from the input text.
   *
   * @param tuple tuple received from TextInputFormat, for each line
   * @return tuple of the text, which was split into integer and string
   * @throws Exception
   */
  @Override
  public Tuple2<Integer, String> map(
    Tuple2<LongWritable, Text> tuple) throws Exception {
    String[] stringArray = tuple.getField(1).toString().split(" ");
    returnTuple.f0 = Integer.parseInt(stringArray[1]);
    returnTuple.f1 = stringArray[0];
    return returnTuple;
  }
}
