package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

import java.util.regex.Pattern;


/**
 * Reads vertex data from a tsv document and creates a Tuple6 Dataset. The
 * document contains at least the source vertex id, a language property of
 * the source vertex, a target vertex id and a language property of the
 * target vertex. All entries should be space separated.
 * <p>
 * Example:
 * <p>
 * 0 EN 1 ZH
 * 2 EN 3 JA
 * 4 EN 5 SV
 * 4 EN 6 SV
 * 7 EN 8 JA
 * 9 SV 8 JA
 */
public class TSVToTuple implements MapFunction<String,
  Tuple6<String, GradoopId, String, String, GradoopId, String>> {
  /**
   * Separator token
   */
  private static final Pattern FILE_SEPARATOR_TOKEN = Pattern.compile(" ");

  /**
   * Reads every line of the tsv document and creates a tuple6 that contains
   * the two origin ids, generated gradoop ids and the language properties
   *
   * @param s           line of tsv document
   * @return            tuple6 containing origin ids, gradoopIds and properties
   * @throws Exception
   */
  @Override
  public Tuple6<String, GradoopId, String, String, GradoopId, String> map(
    String s)
    throws Exception {
    String[] token = FILE_SEPARATOR_TOKEN.split(s);
    return new Tuple6<>(token[0], GradoopId.get(), token[1], token[2],
      GradoopId.get(), token[3]);
  }
}
