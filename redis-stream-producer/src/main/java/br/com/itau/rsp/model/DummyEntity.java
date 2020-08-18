package br.com.itau.rsp.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class DummyEntity {
	private String message;
	private Integer age;
	private Account account;
}
