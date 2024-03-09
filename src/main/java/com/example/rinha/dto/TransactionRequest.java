package com.example.rinha.dto;

import com.example.rinha.BusinessException;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

public record TransactionRequest(@JsonProperty("valor") Integer amount,
                                 @JsonProperty("tipo") String type,
                                 @JsonProperty("descricao") String description) {
    public TransactionRequest {
        if (amount == null || amount < 0) {
            throw new BusinessException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
        if (!type.equals("c") && !type.equals("d")) {
            throw new BusinessException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
        if (!StringUtils.hasText(description) || description.length() > 10) {
            throw new BusinessException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
    }
}
