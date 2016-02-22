package org.infodancer.persist.dbapi;

import java.util.LinkedList;
import java.util.List;

public abstract class QueryParameterGroup
{
	public enum GroupType { AND, OR };
	protected List<QueryParameter> params;
	protected GroupType type;
	
	public QueryParameterGroup(GroupType type)
	{
		this.params = new LinkedList<QueryParameter>();
		this.type = type;
	}

	public GroupType getGroupType()
	{
		return type;
	}
	
	public List<QueryParameter> getQueryParameters()
	{
		return params;
	}
	
	public void addQueryParameter(QueryParameter param)
	{
		params.add(param);
	}

	public void removeQueryParameter(QueryParameter param)
	{
		params.remove(param);
	}
}
