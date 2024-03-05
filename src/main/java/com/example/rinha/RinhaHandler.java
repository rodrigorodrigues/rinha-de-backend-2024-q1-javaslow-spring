package com.example.rinha;

import com.example.rinha.dto.BalanceResponse;
import com.example.rinha.dto.TransactionRequest;
import com.example.rinha.dto.TransactionResponse;
import com.example.rinha.model.Transaction;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
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
import reactor.util.function.Tuple2;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

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

    private final ValidatorFactory factory = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory();
    private final Validator validator = factory.getValidator();

    public RinhaHandler(RinhaRepository rinhaRepository) {
        this.rinhaRepository = rinhaRepository;
    }

    public Mono<ServerResponse> handleGetRequest(ServerRequest request) {
        var accountId = getLimitByAccountId(request).clientId();
        log.debug("handleGetRequest: {}", request);
        return Mono.zip(rinhaRepository.findLastTransactionsByAccountId(accountId).collectList(), rinhaRepository.totalBalanceByAccountId(accountId))
                .flatMap(p -> {
                    var balance = new BalanceResponse.Balance(p.getT2(), Instant.now(), getLimitByAccountId(request).creditLimit());
                    log.debug("getLastTransactions:balance: {}", balance);
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(new BalanceResponse(balance, p.getT1()));
                });
    }

    public Mono<ServerResponse> handlePostRequest(ServerRequest request) {
        var account = getLimitByAccountId(request);
        log.debug("handlePostRequest: {}", request);
        return processRequest(request, account)
                .flatMap(tuple2 -> {
                    var total = tuple2.getT2();
                    var transactionRequest = tuple2.getT1();

                    return rinhaRepository.saveTransaction(new Transaction(transactionRequest, account.clientId(), Instant.now()))
                            .map(t -> new TransactionResponse(account.creditLimit(), total));
                })
                .flatMap(response -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(response));
    }

    private Mono<Tuple2<TransactionRequest, Integer>> processRequest(ServerRequest request, KeyPair account) {
        return request
                .bodyToMono(TransactionRequest.class)
                .zipWhen(transactionRequest -> {
                    Set<ConstraintViolation<TransactionRequest>> errors = validator.validate(transactionRequest);

                    if (!errors.isEmpty()) {
                        log.debug("Bad Request: {}", errors);
                        return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Found invalid data"));
                    } else if (transactionRequest.type().equals("d") && transactionRequest.amount() > account.creditLimit()) {
                        return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Debit is greater than allowed"));
                    }

                    log.debug("issuer:transactionRequest: {}", transactionRequest);

                    return rinhaRepository.totalBalanceByAccountId(account.clientId())
                            .flatMap(total -> {
                                var amount = (transactionRequest.type().equals("d") ? -transactionRequest.amount() : transactionRequest.amount());
                                if (transactionRequest.type().equals("d") && (transactionRequest.amount() + Math.abs(total)) > account.creditLimit()) {
                                    return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY));
                                }
                                return rinhaRepository.updateAccountBalance(amount, account.clientId())
                                        .flatMap(p -> Mono.just(amount + total));
                            });
                });
    }

    private KeyPair getLimitByAccountId(ServerRequest request) {
        Integer accountId = getAccountIdByRequestParam(request);
        if (!accounts.containsKey(accountId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
        return new KeyPair(accountId, accounts.get(accountId));
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

    private record KeyPair(Integer clientId, Integer creditLimit) {}

}
