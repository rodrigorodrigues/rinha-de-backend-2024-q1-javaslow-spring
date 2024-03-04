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
                                row.getLong("dateMillis")),
                accountId,
                Instant.now().minusSeconds(10).toEpochMilli(),
                Instant.now().toEpochMilli());
    }

    public Mono<Transaction> saveTransaction(Transaction transaction) {
        return reactiveCqlTemplate.execute("INSERT INTO rinha.transactions(accountId, type, description, date, amount, dateMillis) VALUES (?, ?, ?, ?, ?, ?)",
                transaction.accountId(),
                        transaction.type(),
                        transaction.description(),
                        transaction.date(),
                        transaction.amount(),
                        transaction.dateMillis())
                .thenReturn(transaction);
    }

    public Mono<Boolean> updateAccountBalance(Integer amount, Integer id) {
        return reactiveCqlTemplate.execute("UPDATE rinha.accounts_balance SET total = total + ? WHERE accountId = ?",
                        (long) amount, id);
    }

    public Mono<Integer> totalBalanceByAccountId(Integer id) {
        return reactiveCqlTemplate.queryForObject("SELECT total FROM rinha.accounts_balance WHERE accountId = ?", Long.class, id)
                .map(Long::intValue);
    }
}
