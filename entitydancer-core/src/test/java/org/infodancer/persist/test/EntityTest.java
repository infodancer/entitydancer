package org.infodancer.persist.test;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.junit.Ignore;

@Ignore
@Entity
public class EntityTest 
{
	Long id;
	String name;
	Float floatValue;
	Double doubleValue;
	Integer intValue;
	Trinary trinaryValue;
	boolean booleanValue;
	java.util.Date dateValue;
	java.sql.Timestamp timestampValue;
	
	public EntityTest()
	{
		this.name = "defaultName";
		this.intValue = 41;
		this.floatValue = 42.0f;
		this.doubleValue = 43.0;
		this.trinaryValue = Trinary.MAYBE;
		this.booleanValue = false;
		this.dateValue = new Date();
		this.timestampValue = new java.sql.Timestamp(dateValue.getTime());
	}

	public java.util.Date getDateValue()
	{
		return dateValue;
	}

	public void setDateValue(java.util.Date dateValue)
	{
		this.dateValue = dateValue;
	}

	public java.sql.Timestamp getTimestampValue()
	{
		return timestampValue;
	}

	public void setTimestampValue(java.sql.Timestamp timestampValue)
	{
		this.timestampValue = timestampValue;
	}

	public boolean isBooleanValue()
	{
		return booleanValue;
	}

	public void setBooleanValue(boolean booleanValue)
	{
		this.booleanValue = booleanValue;
	}

	public Trinary getTrinaryValue() 
	{
		return trinaryValue;
	}

	public void setTrinaryValue(Trinary trinaryValue) 
	{
		this.trinaryValue = trinaryValue;
	}

	public Float getFloatValue() 
	{
		return floatValue;
	}

	public void setFloatValue(Float floatValue) 
	{
		this.floatValue = floatValue;
	}

	public Double getDoubleValue()
	{
		return doubleValue;
	}

	public void setDoubleValue(Double doubleValue) 
	{
		this.doubleValue = doubleValue;
	}

	public Integer getIntValue() 
	{
		return intValue;
	}

	public void setIntValue(Integer intValue) 
	{
		this.intValue = intValue;
	}

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
	
	@Column(length=2048)
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	/** 
	 * For test purposes, checks that all values are equal.
	 */
	public boolean equals(Object o)
	{
		if (o instanceof EntityTest)
		{
			EntityTest value = (EntityTest) o;
			if (id != null) 
			{
				if (!id.equals(value.getId())) return false;
			}
			else if (value.getId() != null) return false;

			if (name != null) 
			{
				if (!id.equals(value.getName())) return false;
			}
			else if (value.getName() != null) return false;

			if (floatValue != null) 
			{
				if (!floatValue.equals(value.getFloatValue())) return false;
			}
			else if (value.getFloatValue() != null) return false;
			 
			if (doubleValue != null) 
			{
				if (!doubleValue.equals(value.getDoubleValue())) return false;
			}
			else if (value.getDoubleValue() != null) return false;

			if (intValue != null) 
			{
				if (!intValue.equals(value.getIntValue())) return false;
			}
			else if (value.getIntValue() != null) return false;
			return true;
		}
		else return false;
	}
}
