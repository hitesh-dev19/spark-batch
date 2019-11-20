package spark.spark;

import java.io.Serializable;

public class SchemaForSpark implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 423342323L;

	private String id;
	private String shopName;
	private String owner;
	private String country;

	public SchemaForSpark(String id, String shopName, String owner,String country) {
		super();
		this.id = id;
		this.shopName = shopName;
		this.owner = owner;
		this.country = country;
	}

	
	
	



	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getShopName() {
		return shopName;
	}

	public void setShopName(String shopName) {
		this.shopName = shopName;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getCountry() {
		return country;
	}



	public void setCountry(String country) {
		this.country = country;
	}

}