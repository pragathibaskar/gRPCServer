package com.example.grpc.server;

import java.util.Date;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;


@Repository
public interface ProvisionDb extends JpaRepository<Provision, Composite>{
	
	@Query(value="select * from provision p where p.periodo_certificacion=?1",
			nativeQuery=true)
      Page<Provision> findByPeriodo(Date date,Pageable page);

}
