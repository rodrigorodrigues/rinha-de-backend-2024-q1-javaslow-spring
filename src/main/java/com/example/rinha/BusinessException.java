package com.example.rinha;

import org.springframework.http.HttpStatus;

public class BusinessException extends RuntimeException {
    private final HttpStatus httpStatus;
    public BusinessException(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int getStatusCode() {
        return httpStatus.value();
    }
}
