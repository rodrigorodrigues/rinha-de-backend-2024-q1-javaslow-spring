package com.example.rinha.dto;

import java.io.Serializable;

public record KeyPairValue<K, V>(K key, V value) implements Serializable {
}
