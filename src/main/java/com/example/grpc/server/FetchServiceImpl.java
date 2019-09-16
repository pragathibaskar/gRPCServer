package com.example.grpc.server;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.cg.grpc.FetchserviceGrpc.FetchserviceImplBase;
import com.cg.grpc.FindProvision;
import com.cg.grpc.GetPage;
import com.cg.grpc.GetPage.Builder;
import com.cg.grpc.GetResults;

import io.grpc.stub.StreamObserver;

@GRpcService
public class FetchServiceImpl extends FetchserviceImplBase {

	@Autowired
	private ProvisionDb db;

	public void fetch(FindProvision request, StreamObserver<GetPage> response) {

		Date date = new Date(request.getTimestamp());
		System.out.println("Request Payload\n"+"Date : "+new Date(request.getTimestamp()));
		System.out.println("Requested time\n"+new Date());
		Pageable listAll = null;
		listAll = PageRequest.of(request.getPage(), request.getSize(), Sort.by("codigo_sap_expediente").ascending());
		Page<Provision> pg = db.findByPeriodo(date, listAll);
		//System.out.println("Inside Response-breakpoint page response" + pg);
		int totalelements = (int) pg.getTotalElements();
		//System.out.println("Inside Response-breakpoint total elements" + totalelements);
		List<Provision> list = pg.getContent();

		GetPage.Builder build = GetPage.newBuilder();
		System.out.println("Respone ");
		System.out.println("codigo_sap_expediente values with cod_sociedad");
		for (int i = 0; i < list.size(); i++) {
			
			System.out.println(list.get(i).getKey().getCodigo()+"   "+list.get(i).getKey().getCod_sociedad());

			build.addResult(GetResults.newBuilder().setCodigo(list.get(i).getKey().getCodigo())
					.setCodSocieded(list.get(i).getKey().getCod_sociedad()).setTimestamp(list.get(i).getTimestamp())
					.build());

		}

		build.setTotalelements(totalelements);
		GetPage b = build.build();
		response.onNext(b);
		response.onCompleted();

	}
}
