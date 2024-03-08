package com.example.rinha;

import com.example.rinha.dto.KeyPairValue;
import com.example.rinha.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public class RinhaRepository {
    private final Logger log = LoggerFactory.getLogger(RinhaRepository.class);
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

    public Mono<Boolean> updateTemporaryAccountBalance(Integer amount, Integer id) {
        log.debug("updateTemporaryAccountBalance: {}={}", amount, id);
        return reactiveCqlTemplate.execute("UPDATE rinha.accounts_balance SET temporary = temporary + ? WHERE accountId = ?",
                (long) amount, id);
    }

    public Mono<Boolean> updateAccountBalance(Integer amount, Integer id) {
        log.debug("Updating updateAccountBalance: {}={}", amount, id);
        return reactiveCqlTemplate.execute("UPDATE rinha.accounts_balance SET total = total + ? WHERE accountId = ?",
                        (long) amount, id);
    }


    public Mono<KeyPairValue<Long, Long>> totalBalanceByAccountId(Integer id) {
        log.debug("Getting totalBalanceByAccountId: {}", id);
        return reactiveCqlTemplate.queryForObject("SELECT temporary, total FROM rinha.accounts_balance WHERE accountId = ?",
                        (row, rowNum) -> new KeyPairValue<>(row.getLong("temporary"), row.getLong("total")),
                        id);
    }
}
