package com.example.rinha.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;

public record TransactionRequest(@JsonProperty("valor") Integer amount,
                                 @JsonProperty("tipo") String type,
                                 @JsonProperty("descricao") String description) {
    public TransactionRequest {
        if (amount == null || amount < 0) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid data");
        }
        if (!type.equals("c") && !type.equals("d")) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid data");
        }
        if (!StringUtils.hasText(description) || description.length() > 10) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid data");
        }
    }
}
