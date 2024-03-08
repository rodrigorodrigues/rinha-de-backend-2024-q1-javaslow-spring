package com.example.rinha;

import com.example.rinha.dto.BalanceResponse;
import com.example.rinha.dto.KeyPairValue;
import com.example.rinha.dto.TransactionRequest;
import com.example.rinha.dto.TransactionResponse;
import com.example.rinha.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;

@Component
public class RinhaHandler {
    private final Logger log = LoggerFactory.getLogger(RinhaHandler.class);
    private final RinhaRepository rinhaRepository;
    private final Map<Integer, Integer> accounts = Map.ofEntries(
            new AbstractMap.SimpleEntry<>(1, 100000),
            new AbstractMap.SimpleEntry<>(2, 80000),
            new AbstractMap.SimpleEntry<>(3, 1000000),
            new AbstractMap.SimpleEntry<>(4, 10000000),
            new AbstractMap.SimpleEntry<>(5, 500000)
    );
    public RinhaHandler(RinhaRepository rinhaRepository) {
        this.rinhaRepository = rinhaRepository;
    }

    public Mono<ServerResponse> handleGetRequest(ServerRequest request) {
        var accountId = getLimitByAccountId(request).key();
        log.debug("handleGetRequest: {}", request);
        return Mono.zip(rinhaRepository.totalBalanceByAccountId(accountId), rinhaRepository.findLastTransactionsByAccountId(accountId).collectList())
                .flatMap(p -> {
                    var balance = new BalanceResponse.Balance(p.getT1().value(), Instant.now(), getLimitByAccountId(request).value());
                    log.debug("getLastTransactions:balance: {}", balance);
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(new BalanceResponse(balance, p.getT2()));
                });
    }

    public Mono<ServerResponse> handlePostRequest(ServerRequest request) {
        var account = getLimitByAccountId(request);
        log.debug("handlePostRequest: {}", request);
        var clientId = account.key();
        return Mono.zip(processRequest(request, account), rinhaRepository.totalBalanceByAccountId(clientId))
                .flatMap(tuple -> {
                    var total = tuple.getT2().value();
                    var temporaryTotal = tuple.getT2().key();
                    var transactionRequest = tuple.getT1();

                    var amount = transactionRequest.amount();

                    if (transactionRequest.type().equals("d")) {
                        if ((amount + Math.abs(temporaryTotal)) > account.value()) {
                            return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY));
                        }
                        amount = -amount;
                    }

                    var totalBalance = total + amount;
                    return Mono.zip(rinhaRepository.saveTransaction(new Transaction(transactionRequest, account.key(), Instant.now())), rinhaRepository.updateAccountBalance(amount, clientId))
                            .map(t -> new TransactionResponse(account.value(), totalBalance));
                })
                .flatMap(response -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(response));
    }

    private Mono<TransactionRequest> processRequest(ServerRequest request, KeyPairValue<Integer, Integer> account) {
        return request
                .bodyToMono(TransactionRequest.class)
                .flatMap(transactionRequest -> {
                    var creditLimit = account.value();
                    var clientId = account.key();

                    if (transactionRequest.type().equals("d") && transactionRequest.amount() > creditLimit) {
                        return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Debit is greater than allowed"));
                    }

                    log.debug("issuer:transactionRequest: {}", transactionRequest);

                    var amount = (transactionRequest.type().equals("d") ? -transactionRequest.amount() : transactionRequest.amount());
                    return rinhaRepository.updateTemporaryAccountBalance(amount, clientId)
                            .map(p -> transactionRequest);
                });
    }

    private KeyPairValue<Integer, Integer> getLimitByAccountId(ServerRequest request) {
        Integer accountId = getAccountIdByRequestParam(request);
        if (!accounts.containsKey(accountId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
        return new KeyPairValue<>(accountId, accounts.get(accountId));
    }

    private Integer getAccountIdByRequestParam(ServerRequest request) {
        try {
            return Integer.parseInt(request.pathVariable("accountId"));
        } catch (NumberFormatException nfe) {
            log.warn("Invalid data", nfe);
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY);
        }
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<Map<String, String>> businessException(Exception e) {
        log.warn("Invalid data", e);
        return ResponseEntity.unprocessableEntity()
                .body(Collections.singletonMap("error", e.getLocalizedMessage()));
    }

}
