package org.gradoop.flink.io.impl.rdbms.connection;

import java.sql.JDBCType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import com.google.inject.util.Types;

/**
 * maps given JDBCType to a flink specific BasicTypeInfo
 * @author pc
 *
 */
public class SQLToBasicTypeMapper {
	public SQLToBasicTypeMapper() {
	}

	public static BasicTypeInfo getBasicTypeInfo(JDBCType jdbcType) {
		BasicTypeInfo bti = null;
		switch (jdbcType.name()) {
		case "CHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "VARCHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "LONGVARCHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "NUMERIC":
			bti = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "DECIMAL":
			bti = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "BIT":
			bti = BasicTypeInfo.BOOLEAN_TYPE_INFO;
			break;
		case "TINYINT":
			bti = BasicTypeInfo.BYTE_TYPE_INFO;
			break;
		case "SMALLINT":
			bti = BasicTypeInfo.SHORT_TYPE_INFO;
			break;
		case "INTEGER":
			bti = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "BIGINT":
			bti = BasicTypeInfo.LONG_TYPE_INFO;
			break;
		case "REAL":
			bti = BasicTypeInfo.FLOAT_TYPE_INFO;
			break;
		case "FLOAT":
			bti = BasicTypeInfo.DOUBLE_TYPE_INFO;
			break;
		case "DOUBLE":
			bti = BasicTypeInfo.DOUBLE_TYPE_INFO;
			break;
		case "BINARY":
			System.err.println("No Typemapping for Type : BINARY");
			break;
		case "VARBINARY":
			System.err.println("No Typemapping for Type : VARBINARY");
			break;
		case "LONGVARBINARY":
			System.err.println("No Typemapping for Type : LONGVARBINARY");
			break;
		case "DATE":
			bti = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "TIME":
			bti = BasicTypeInfo.VOID_TYPE_INFO;
			break;
		case "TIMESTAMP":
			bti = BasicTypeInfo.VOID_TYPE_INFO;
			break;
		case "CLOB":
			System.err.println("No Typemapping for Type : CLOB");
			break;
		case "BLOB":
			System.err.println("No Typemapping for Type : BLOB");
			break;
		case "ARRAY":
			System.err.println("No Typemapping for Type : ARRAY");
			break;
		case "DINSTINCT":
			System.err.println("No Typemapping for Type : DISTINCT");
			break;
		case "STRUCT":
			System.err.println("No Typemapping for Type : STRUCT");
			break;
		case "REF":
			System.err.println("No Typemapping for Type : REF");
			break;
		case "JAVA_OBJECT":
			System.err.println("No Typemapping for Type : JAVA_OBJECT");
			break;
		}

		return bti;
	}
}
