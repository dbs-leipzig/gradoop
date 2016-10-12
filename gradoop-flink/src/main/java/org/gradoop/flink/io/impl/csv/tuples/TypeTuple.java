package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple;

import java.util.List;

/**
 * Created by stephan on 11.10.16.
 */
public class TypeTuple extends Tuple {

  public TypeTuple() {
  }

  @Override
  public Object getField(int i) {
    return null;
  }

  @Override
  public <T> void setField(T t, int i) {

  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public <T extends Tuple> T copy() {
    return null;
  }
}
