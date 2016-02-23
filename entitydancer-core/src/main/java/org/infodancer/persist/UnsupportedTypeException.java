package org.infodancer.persist;

import javax.persistence.PersistenceException;

public class UnsupportedTypeException extends PersistenceException 
{
	public UnsupportedTypeException()
	{
		super();
	}

	public UnsupportedTypeException(String msg)
	{
		super(msg);
	}
}
