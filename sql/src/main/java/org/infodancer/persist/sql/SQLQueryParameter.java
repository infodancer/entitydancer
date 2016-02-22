package org.infodancer.persist.sql;

import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;

public class SQLQueryParameter extends QueryParameter
{
	public SQLQueryParameter(String left, String type, String right)
	{
		super(left, type, right);
	}

	public SQLQueryParameter(String left, QueryParameterType type, String right)
	{
		super(left, type, right);
	}

	public String toString()
	{
		StringBuilder q = new StringBuilder();
		q.append(parseValue(left));
		q.append(decodeType(type));
		q.append(parseValue(right));
		return q.toString();
	}
	
	private String parseValue(String value)
	{
		return value;
	}
}
