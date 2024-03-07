package com.example.rinha.dto;

import java.io.Serializable;

public record KeyPairValue(Integer clientId, Integer creditLimit) implements Serializable {
}
