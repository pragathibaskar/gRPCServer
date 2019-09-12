package com.example.grpc.server;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

@Entity
@Table(name="provision",
       uniqueConstraints= {@UniqueConstraint(columnNames= {"codigo_sap_expediente","cod_sociedad","periodo_certificacion"})})
public class Provision {
	
	@EmbeddedId
	private Composite key;

	public Composite getKey() {
		return key;
	}

	public void setKey(Composite key) {
		this.key = key;
	}
	
	@Transient
	private long timestamp;

	public long getTimestamp() {
		return getKey().getPeriodo().getTime();
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
}