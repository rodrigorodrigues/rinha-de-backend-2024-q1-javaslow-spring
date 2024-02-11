package com.example.rinha;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"spring.cassandra.keyspace-name=rinha",
        "logging.level.org.springframework.web=trace", "server.error.include-message=always", "server.error.include-exception=true"})
@AutoConfigureMockMvc
@Testcontainers
class RinhaApplicationTests {
    @Autowired
    MockMvc mockMvc;
    @Autowired
    ObjectMapper objectMapper;

    @Container
    @ServiceConnection
    static CassandraContainer cassandraContainer = new CassandraContainer("cassandra:latest")
            .withInitScript("schema.cql");

    @Test
    void contextLoads() throws Exception {
        mockMvc.perform(post("/clientes/1/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AccountController.TransactionRequest(100, "c", "Test"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limite").isNotEmpty())
                .andExpect(jsonPath("$.saldo").isNotEmpty());

        mockMvc.perform(post("/clientes/1/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AccountController.TransactionRequest(500, "c", "Test"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limite").isNotEmpty())
                .andExpect(jsonPath("$.saldo").isNotEmpty());

        mockMvc.perform(post("/clientes/4/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AccountController.TransactionRequest(10000001, "d", "Test"))))
                .andExpect(status().isUnprocessableEntity());

        mockMvc.perform(post("/clientes/6/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AccountController.TransactionRequest(500, "c", "Test"))))
                .andExpect(status().isNotFound());

        mockMvc.perform(post("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AccountController.TransactionRequest(-10, "a", null))))
                .andExpect(status().is(422));

        mockMvc.perform(post("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"valor\": 1.2, \"tipo\": \"d\", \"descricao\": \"devolve\"}"))
                .andExpect(status().is(422));

        mockMvc.perform(post("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"valor\": 1, \"tipo\": \"x\", \"descricao\": \"devolve\"}"))
                .andExpect(status().is(422));

        mockMvc.perform(post("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"valor\": 1, \"tipo\": \"c\", \"descricao\": \"123456789 e mais um pouco\"}"))
                .andExpect(status().is(422));

        String response = mockMvc.perform(get("/clientes/1/extrato"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.saldo.limite").isNotEmpty())
                .andExpect(jsonPath("$.saldo.data_extrato").isNotEmpty())
                .andExpect(jsonPath("$.saldo.total").value(600))
                .andExpect(jsonPath("$.ultimas_transacoes").isArray())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertThat(response).isNotBlank();

        AccountController.AccountTransactionsResponse accountTransactionsResponse = objectMapper.readValue(response, AccountController.AccountTransactionsResponse.class);

        assertThat(accountTransactionsResponse).isNotNull();

        assertThat(accountTransactionsResponse.lastTransactions().stream().map(Transaction::date)).isSortedAccordingTo(Comparator.reverseOrder());

        mockMvc.perform(get("/clientes/6/extrato"))
                .andExpect(status().isNotFound());

        mockMvc.perform(post("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"valor\": 150, \"tipo\": \"c\", \"descricao\": \"hzdFsFQa1W\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limite").isNotEmpty())
                .andExpect(jsonPath("$.saldo").isNotEmpty());
    }

}
