package org.infodancer.persist.test;

import java.sql.Blob;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class BlobEntity
{
	Long id;
	Blob binaryValue;
	
	@Id
	@GeneratedValue
	public Long getId()
	{
		return id;
	}
	
	public void setId(Long id)
	{
		this.id = id;
	}

	public Blob getBinaryValue()
	{
		return binaryValue;
	}

	public void setBinaryValue(Blob binaryValue)
	{
		this.binaryValue = binaryValue;
	}
	
}
