package com.example.rinha;

import com.example.rinha.model.Transaction;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public class RinhaRepository {
    private final ReactiveCqlTemplate reactiveCqlTemplate;

    public RinhaRepository(ReactiveCqlTemplate reactiveCqlTemplate) {
        this.reactiveCqlTemplate = reactiveCqlTemplate;
    }

    public Flux<Transaction> findLastTransactionsByAccountId(Integer accountId) {
        return reactiveCqlTemplate.query("SELECT type, description, date, amount, dateMillis FROM rinha.TRANSACTIONS WHERE accountId = ? AND dateMillis >= ? and dateMillis <= ? LIMIT 10",
                (row, rowNum) ->
                        new Transaction(accountId,
                                row.getString("type"),
                                row.getString("description"),
                                row.getInstant("date"),
                                row.getInt("amount"),
                                row.getLong("dateMillis"),
                                -1),
                accountId,
                Instant.now().minusSeconds(10).toEpochMilli(),
                Instant.now().toEpochMilli());
    }

    public Mono<Transaction> saveTransaction(Transaction transaction) {
        return reactiveCqlTemplate.execute("INSERT INTO rinha.transactions(accountId, type, description, date, amount, dateMillis, lastStatementBalance) VALUES (?, ?, ?, ?, ?, ?, ?)",
                transaction.accountId(),
                        transaction.type(),
                        transaction.description(),
                        transaction.date(),
                        transaction.amount(),
                        transaction.dateMillis(),
                        transaction.lastStatementBalance())
                .thenReturn(transaction);
    }

    public Mono<Integer> totalBalanceByAccountId(Integer id) {
        return reactiveCqlTemplate.queryForObject("SELECT lastStatementBalance FROM rinha.transactions WHERE accountId = ? LIMIT 1", Integer.class,
                        id)
                .switchIfEmpty(Mono.just(0));
    }
}
