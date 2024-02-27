package com.example.rinha.dto;

import com.example.rinha.model.Transaction;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

public record BalanceResponse(@JsonProperty("saldo") Balance balance,
                              @JsonProperty("ultimas_transacoes") List<Transaction> lastTransactions) {
    public record Balance(Integer total, @JsonProperty("data_extrato") Instant instant,
                          @JsonProperty("limite") Integer creditLimit) {
    }
}
