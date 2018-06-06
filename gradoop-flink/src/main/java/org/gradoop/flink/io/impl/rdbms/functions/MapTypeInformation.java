package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.JDBCType;
import java.sql.SQLInput;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class MapTypeInformation {

	public MapTypeInformation() {
	}

	public static RowTypeInfo getRowTypeInfo(RDBMSTable table) throws ClassNotFoundException {
		int i = 0;
		TypeInformation[] fieldTypes = new TypeInformation[table.getSqlTypes().size()];
		
		for(JDBCType f : table.getSqlTypes()){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(f);
			i++;
		}
	
		return new RowTypeInfo(fieldTypes);
	}
}
