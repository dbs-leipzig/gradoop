package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.api.EPGMLabeled;

/**
 * epgmLabeled => label
 *
 * @param <L> label type
 */
public class Label<L extends EPGMLabeled> implements KeySelector<L, String> {

  @Override
  public String getKey(L l) throws Exception {
    return l.getLabel();
  }
}
