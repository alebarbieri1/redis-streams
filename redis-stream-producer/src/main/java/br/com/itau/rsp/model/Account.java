package br.com.itau.rsp.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class Account {
	private String name;
	private Integer number;
}