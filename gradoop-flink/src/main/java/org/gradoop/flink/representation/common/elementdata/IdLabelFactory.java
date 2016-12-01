package org.gradoop.flink.representation.common.elementdata;

import org.gradoop.common.model.impl.pojo.Element;


public class IdLabelFactory implements ElementDataFactory<IdLabel> {

  @Override
  public IdLabel createValue(Element element) {
    return new IdLabel(element.getId(), element.getLabel());
  }
}
