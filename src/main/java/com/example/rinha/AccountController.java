package com.example.rinha;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.Length;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
public class AccountController {
    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;

    public AccountController(AccountRepository accountRepository, TransactionRepository transactionRepository) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
    }

    @PostMapping(value = "/clientes/{accountId}/transacoes", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TransactionResponse> issuer(@RequestBody @Valid TransactionRequest transactionRequest, @PathVariable Integer accountId) {
        var account = checkAccountExists(accountId);
        var balance = account.balance();
        if (transactionRequest.type().equals("c")) {
            balance += transactionRequest.amount;
        } else {
            balance -= transactionRequest.amount();
            if (account.creditLimit() + balance < 0) {
                throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY);
            }
        }
        accountRepository.save(account.withBalance(balance));
        transactionRepository.save(new Transaction(accountId, transactionRequest.type, transactionRequest.description, Instant.now(), transactionRequest.amount()));
        return ResponseEntity.ok(new TransactionResponse(account));
    }

    @GetMapping(value = "/clientes/{accountId}/extrato", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AccountTransactionsResponse> getLastTransactions(@PathVariable Integer accountId) {
        var account = checkAccountExists(accountId);
        var transactions = transactionRepository.findLastTransactionsByAccountId(accountId);
        var total = transactions.stream().mapToInt(Transaction::amount).sum();
        var balance = new Balance(total, Instant.now(), account.creditLimit());
        return ResponseEntity.ok(new AccountTransactionsResponse(balance, transactions));
    }

    private Account checkAccountExists(Integer accountId) {
        var optionalAccount = accountRepository.findById(accountId);
        if (optionalAccount.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, String.format("Usuario com id: %s nao encontrado", accountId));
        }
        return optionalAccount.get();
    }

    public record AccountTransactionsResponse(@JsonProperty("saldo") Balance balance, @JsonProperty("ultimas_transacoes") List<Transaction> lastTransactions) {
    }

    public record Balance(Integer total, @JsonProperty("data_extrato") Instant instant, @JsonProperty("limite") Integer creditLimit) {
    }

    public record TransactionRequest(@NotNull @Min(0) @JsonProperty("valor") Integer amount, @NotNull @Pattern(regexp = "[c|d]") @JsonProperty("tipo") String type,
                                     @NotBlank @Length(max = 10) @JsonProperty("descricao") String description) {
    }

    public record TransactionResponse(@JsonProperty("limite") Integer creditLimit, @JsonProperty("saldo") Integer balance) {
        public TransactionResponse(Account account) {
            this(account.creditLimit(), account.balance());
        }
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<Map<String, String>> badRequestException(Exception e) {
        return ResponseEntity.unprocessableEntity()
                .body(Collections.singletonMap("error", e.getLocalizedMessage()));
    }
}
