package org.infodancer.persist.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Version;

@Entity
public class Address 
{
	private Long id;
	private int version;
	private String street;

	@Id @GeneratedValue
	public Long getId() { return id; }
	public void setId(Long id) { this.id = id; }
	
	@Version
	public int getVersion() { return version; }
	public void setVersion(int version) {
	    this.version = version;
	}
	
	public String getStreet() { return street; }
	public void setStreet(String street) {
	    this.street = street;
	}
}
