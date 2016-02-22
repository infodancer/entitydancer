package org.infodancer.persist.test;

import java.util.ArrayList;
import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;

/** 
 * Used to test ManyToMany relationships.
 * @author matthew
 */
@Entity
public class Car 
{
	private Long id;
	private Integer year;
	private String make;
	private String model;
	private Collection<Driver> drivers = new ArrayList<Driver>();
	
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
	
	public Integer getYear() 
	{
		return year;
	}
	
	public void setYear(Integer year) 
	{
		this.year = year;
	}
	
	public String getMake() 
	{
		return make;
	}
	
	public void setMake(String make) 
	{
		this.make = make;
	}
	
	public String getModel() 
	{
		return model;
	}
	
	public void setModel(String model) 
	{
		this.model = model;
	}
	
	@ManyToMany
	public Collection<Driver> getDrivers() 
	{
		return drivers;
	}
	
	public void setDrivers(Collection<Driver> drivers) 
	{
		this.drivers = drivers;
	}
	
}
