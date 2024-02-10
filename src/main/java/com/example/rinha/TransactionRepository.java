package com.example.rinha;

import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TransactionRepository extends CrudRepository<Transaction, Integer> {
    @Query("SELECT * FROM TRANSACTIONS WHERE accountId = :accountId LIMIT 10")
    List<Transaction> findLastTransactionsByAccountId(@Param("accountId") Integer accountId);
}
