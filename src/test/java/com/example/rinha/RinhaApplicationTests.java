package com.example.rinha;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.rinha.dto.BalanceResponse;
import com.example.rinha.dto.TransactionRequest;
import com.example.rinha.dto.TransactionResponse;
import com.example.rinha.model.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.springframework.web.reactive.function.BodyInserters.fromValue;

@SpringBootTest(properties = {
        "spring.cassandra.keyspace-name=rinha",
        "logging.level.org.springframework.web=trace",
        "logging.level.com.example.rinha=trace",
        "server.error.include-message=always",
        "server.error.include-exception=true"
})
@AutoConfigureWebTestClient(timeout = "1s")
@Testcontainers
class RinhaApplicationTests {
    @Autowired
    WebTestClient client;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    CqlSession session;

    @Container
    @ServiceConnection
    static CassandraContainer<?> cassandraContainer = new CassandraContainer<>("cassandra:latest")
            .withInitScript("schema.cql");

    @AfterEach
    void tearDown() {
        session.execute("TRUNCATE rinha.transactions");
        session.execute("TRUNCATE rinha.accounts_balance");

        session.execute("UPDATE rinha.accounts_balance SET total = total + 0 WHERE accountId = 1");
        session.execute("UPDATE rinha.accounts_balance SET total = total + 0 WHERE accountId = 2");
        session.execute("UPDATE rinha.accounts_balance SET total = total + 0 WHERE accountId = 3");
        session.execute("UPDATE rinha.accounts_balance SET total = total + 0 WHERE accountId = 4");
        session.execute("UPDATE rinha.accounts_balance SET total = total + 0 WHERE accountId = 5");
    }

    @Test
    void contextLoads() throws Exception {
        client.get().uri("/health")
                .exchange()
                .expectStatus().isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.status").value(equalTo("UP"));

        client.post().uri("/clientes/1/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(objectMapper.writeValueAsString(new TransactionRequest(100, "c", "Test"))))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.limite").isNotEmpty()
                .jsonPath("$.saldo").isNotEmpty();

        client.post().uri("/clientes/1/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(objectMapper.writeValueAsString(new TransactionRequest(500, "c", "Test"))))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.limite").isNotEmpty()
                .jsonPath("$.saldo").isNotEmpty();

        client.post().uri("/clientes/4/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(objectMapper.writeValueAsString(new TransactionRequest(10000001, "d", "Test"))))
                .exchange()
                .expectStatus().is4xxClientError();

        client.post().uri("/clientes/6/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(objectMapper.writeValueAsString(new TransactionRequest(500, "c", "Test"))))
                .exchange()
                .expectStatus().isNotFound();

        client.post().uri("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(objectMapper.writeValueAsString(new TransactionRequest(-10, "a", null))))
                .exchange()
                .expectStatus().is4xxClientError();

        client.post().uri("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue("{\"valor\": 1.2, \"tipo\": \"d\", \"descricao\": \"devolve\"}"))
                .exchange()
                .expectStatus().is4xxClientError();

        client.post().uri("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue("{\"valor\": 1, \"tipo\": \"x\", \"descricao\": \"devolve\"}"))
                .exchange()
                .expectStatus().is4xxClientError();

        client.post().uri("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue("{\"valor\": 1, \"tipo\": \"c\", \"descricao\": \"123456789 e mais um pouco\"}"))
                .exchange()
                .expectStatus().is4xxClientError();

        String response = new String(client.get().uri("/clientes/1/extrato")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.saldo.limite").isNotEmpty()
                .jsonPath("$.saldo.data_extrato").isNotEmpty()
                .jsonPath("$.saldo.total").value(equalTo(600))
                .jsonPath("$.ultimas_transacoes").isArray()
                .returnResult()
                .getResponseBody(), StandardCharsets.UTF_8);

        assertThat(response).isNotBlank();

        BalanceResponse balanceResponse = objectMapper.readValue(response, BalanceResponse.class);

        assertThat(balanceResponse).isNotNull();

        assertThat(balanceResponse.lastTransactions().stream().map(Transaction::date)).isSortedAccordingTo(Comparator.reverseOrder());

        client.get().uri("/clientes/6/extrato")
                .exchange()
                .expectStatus().isNotFound();

        client.post().uri("/clientes/5/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue("{\"valor\": 150, \"tipo\": \"c\", \"descricao\": \"hzdFsFQa1W\"}"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.limite").isNotEmpty()
                .jsonPath("$.saldo").isNotEmpty();
    }

    @Test
    void testConcurrency() {
        var transactionResponse = client.post().uri("/clientes/1/transacoes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue("{\"valor\": 1, \"tipo\": \"c\", \"descricao\": \"danada\"}"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(TransactionResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(transactionResponse).isNotNull();
        assertThat(transactionResponse.creditLimit()).isNotNull();
        assertThat(transactionResponse.balance()).isNotNull();

        await().during(Duration.ofSeconds(5)).until(() -> {
            client.get().uri("/clientes/1/extrato")
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.saldo.limite").value(equalTo(transactionResponse.creditLimit()))
                    .jsonPath("$.saldo.data_extrato").isNotEmpty()
                    .jsonPath("$.saldo.total").value(equalTo(transactionResponse.balance()))
                    .jsonPath("$.ultimas_transacoes[0].tipo").value(equalTo("c"))
                    .jsonPath("$.ultimas_transacoes[0].valor").value(equalTo(1))
                    .jsonPath("$.ultimas_transacoes[0].descricao").value(equalTo("danada"));
            return true;
        });
    }

    @Test
    void testDebitCreditMixed() {
        var counter = new AtomicInteger(0);
        var last = new AtomicReference<TransactionResponse>();
        var totalTransactions = 25;
        IntStream.range(0, totalTransactions).forEach(p -> {
            last.set(client.post().uri("/clientes/1/transacoes")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(fromValue("{\"valor\": 1, \"tipo\": \"c\", \"descricao\": \"danada\"}"))
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody(TransactionResponse.class)
                    .returnResult()
                    .getResponseBody());
            counter.incrementAndGet();
        });
        await().untilAtomic(counter, equalTo(totalTransactions));

        assertThat(last.get()).isNotNull();

        var counterLastTransactions = new AtomicInteger(10);
        await().during(Duration.ofSeconds(5)).until(() -> {
            int i = counterLastTransactions.decrementAndGet();
            if (i < 0) {
                counterLastTransactions.set(10);
                i = counterLastTransactions.decrementAndGet();
            }
            client.get().uri("/clientes/1/extrato")
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.saldo.limite").value(equalTo(last.get().creditLimit()))
                    .jsonPath("$.saldo.data_extrato").isNotEmpty()
                    .jsonPath("$.saldo.total").value(equalTo(totalTransactions))
                    .jsonPath(String.format("$.ultimas_transacoes[%s].tipo", i)).value(equalTo("c"))
                    .jsonPath(String.format("$.ultimas_transacoes[%s].valor", i)).value(equalTo(1))
                    .jsonPath(String.format("$.ultimas_transacoes[%s].descricao", i)).value(equalTo("danada"));
            return true;
        });

        var counterDebit = new AtomicInteger(0);
        var lastDebit = new AtomicReference<TransactionResponse>();

        IntStream.range(0, totalTransactions).forEach(p -> {
            lastDebit.set(client.post().uri("/clientes/1/transacoes")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(fromValue("{\"valor\": 1, \"tipo\": \"d\", \"descricao\": \"danada\"}"))
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody(TransactionResponse.class)
                    .returnResult()
                    .getResponseBody());
            counterDebit.incrementAndGet();
        });
        await().untilAtomic(counterDebit, equalTo(totalTransactions));

        assertThat(lastDebit.get()).isNotNull();

        await().during(Duration.ofSeconds(5)).until(() -> {
            client.get().uri("/clientes/1/extrato")
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.saldo.limite").value(equalTo(lastDebit.get().creditLimit()))
                    .jsonPath("$.saldo.data_extrato").isNotEmpty()
                    .jsonPath("$.saldo.total").value(equalTo(0));
            return true;
        });
    }

}
