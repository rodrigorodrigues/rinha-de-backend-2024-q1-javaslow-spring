package com.example.rinha.model;

import com.example.rinha.dto.TransactionRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.Length;
import org.springframework.data.annotation.Id;

import java.time.Instant;

public record Transaction(@Id @JsonIgnore @NotNull Integer accountId,
                    @NotBlank @JsonProperty("tipo") String type,
                   @NotBlank @Length(max = 10) @JsonProperty("descricao") String description,
                    @NotNull @JsonProperty("realizada_em") Instant date,
                          @NotNull @JsonProperty("valor") Integer amount,
                          @JsonIgnore @NotNull Long dateMillis,
                          @JsonIgnore @NotNull Integer lastStatementBalance) {
    public Transaction(TransactionRequest transactionRequest, Integer accountId, Instant date, Integer lastStatementBalance) {
        this(accountId,
                transactionRequest.type(),
                transactionRequest.description(),
                date,
                transactionRequest.amount(),
                date.toEpochMilli(),
                lastStatementBalance);
    }
}
