package org.infodancer.persist.dbapi;

public abstract class QueryParameter
{
	protected String left;
	protected QueryParameterType type;
	protected String right;
	
	public QueryParameter(String left, String type, String right)
	{
		this.left = left;
		this.type = encodeType(type);
		this.right = right;
	}

	public QueryParameterType encodeType(String type)
	{
		if (type.equals("=")) return QueryParameterType.EQUAL;
		else if (type.equals("<")) return QueryParameterType.LESS_THAN;
		else if (type.equals("<=")) return QueryParameterType.LESS_THAN_OR_EQUAL;
		else if (type.equals(">")) return QueryParameterType.GREATER_THAN;
		else if (type.equals(">=")) return QueryParameterType.GREATER_THAN_OR_EQUAL;
		return null;
	}

	public String decodeType(QueryParameterType type)
	{
		switch (type)
		{
			case EQUAL: return "=";
			case LESS_THAN: return "<";
			case LESS_THAN_OR_EQUAL: return "<=";
			case GREATER_THAN_OR_EQUAL: return ">=";
			case GREATER_THAN: return ">";
			default: return "";
		}
	}
	
	public QueryParameter(String left, QueryParameterType type, String right)
	{
		this.left = left;
		this.type = type;
		this.right = right;
	}
	
	public String getLeftValue()
	{
		return left;
	}
	
	public QueryParameterType getQueryParameterType()
	{
		return type;
	}
	
	public String getRightValue()
	{
		return right;
	}
}
