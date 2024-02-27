package com.example.rinha.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.Length;

public record TransactionRequest(@NotNull @Min(0) @JsonProperty("valor") Integer amount,
                                 @NotNull @Pattern(regexp = "[c|d]") @JsonProperty("tipo") String type,
                                 @NotBlank @Length(max = 10) @JsonProperty("descricao") String description) {
}
