package com.example.rinha.model;

import com.example.rinha.dto.TransactionRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record Transaction(@JsonIgnore Integer accountId,
                    @JsonProperty("tipo") String type,
                   @JsonProperty("descricao") String description,
                   @JsonProperty("realizada_em") Instant date,
                          @JsonProperty("valor") Integer amount,
                          @JsonIgnore Long dateMillis) {
    public Transaction(TransactionRequest transactionRequest, Integer accountId, Instant date) {
        this(accountId,
                transactionRequest.type(),
                transactionRequest.description(),
                date,
                transactionRequest.amount(),
                date.toEpochMilli());
    }
}
