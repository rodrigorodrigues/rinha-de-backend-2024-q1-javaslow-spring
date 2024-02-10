package com.example.rinha;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.Length;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;

@Table("transactions")
public record Transaction(@Id @JsonIgnore @NotNull Integer accountId,
                    @NotBlank @JsonProperty("tipo") String type,
                   @NotBlank @Length(max = 10) @JsonProperty("descricao") String description,
                    @NotNull @JsonProperty("realizada_em") Instant date,
                          @NotNull @JsonProperty("valor") Integer amount) {
}
