package com.example.rinha.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TransactionResponse(@JsonProperty("limite") Integer creditLimit, @JsonProperty("saldo") Long balance) {
}
