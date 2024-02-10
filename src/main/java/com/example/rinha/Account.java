package com.example.rinha;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Table;

@Table(value = "accounts")
public record Account(@Id Integer id, @NotNull @JsonProperty("limite") Integer creditLimit, @JsonProperty("saldo") @NotNull Integer balance) {
        public Account withBalance(Integer balance) {
                return new Account(id, creditLimit, balance);
        }
}
