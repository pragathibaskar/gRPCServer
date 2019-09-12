package com.example.grpc.server;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.fasterxml.jackson.annotation.JsonFormat;



@Embeddable
public class Composite implements Serializable{
	
	@Column(name="codigo_sap_expediente")
	private String codigo;
	
	
	public String getCodigo() {
		return codigo;
	}


	public void setCodigo(String codigo) {
		this.codigo = codigo;
	}


	public String getCod_sociedad() {
		return cod_sociedad;
	}


	public void setCod_sociedad(String cod_sociedad) {
		this.cod_sociedad = cod_sociedad;
	}


	public Date getPeriodo() {
		return periodo;
	}


	public void setPeriodo(Date periodo) {
		this.periodo = periodo;
	}


	@Column(name="cod_sociedad",columnDefinition="varchar(4) default '2000'")
    private String cod_sociedad;
	
	
	@Column(name="periodo_certificacion")
	@JsonFormat(pattern="dd/MM/yyyy")
	private Date periodo;

}
