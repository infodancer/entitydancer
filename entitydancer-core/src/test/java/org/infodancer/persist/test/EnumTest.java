package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class EnumTest
{
	Long id;
	Trinary trinary;
	
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
	
	public Trinary getTrinary()
	{
		return trinary;
	}
	
	public void setTrinary(Trinary enumvalue)
	{
		this.trinary = enumvalue;
	}
}
