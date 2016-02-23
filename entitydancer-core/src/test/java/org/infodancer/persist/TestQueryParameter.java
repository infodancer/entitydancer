package org.infodancer.persist;

import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;

public class TestQueryParameter extends QueryParameter
{
	public TestQueryParameter(String left, String type, String right)
	{
		super(left, type, right);
	}
	
	public TestQueryParameter(String left, QueryParameterType type, String right)
	{
		super(left, type, right);
	}
}
