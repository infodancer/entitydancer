package org.infodancer.persist.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Version;

@Entity
public class Customer 
{
	Long id;
	protected int version;
	Address address;
	String description;
	Collection<Purchase> orders = new ArrayList<Purchase>();
	Collection<DeliveryService> serviceOptions = new HashSet<DeliveryService>();
	
	@Id @GeneratedValue
	public Long getId() { return id; }
	public void setId(Long id) { this.id = id; }
	
	@Version
	public int getVersion() { return version; }
	
	public void setVersion(int version) 
	{
	    this.version = version;
	}
	
	@ManyToOne
	public Address getAddress() { return address; }
	
	public void setAddress(Address addr) 
	{
	    this.address = addr;
	}
	
	@Basic
	public String getDescription() { return description; }
	
	public void setDescription(String desc) {
	    this.description = desc;
	}
	
	@OneToMany
	public Collection<Purchase> getOrders() { return orders; }
	public void setOrders(Collection<Purchase> orders) { this.orders = orders; }
	
	@ManyToMany
	public Collection<DeliveryService> getServiceOptions() 
	{
	    return serviceOptions;
	}
	
	public void setServiceOptions(Collection<DeliveryService> options)
	{
		this.serviceOptions = options;
	}
}
