package com.example.rinha;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.example.rinha.dto.TransactionRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;

@SpringBootApplication
@RegisterReflectionForBinding(TransactionRequest.class)
public class RinhaApplication {

    public static void main(String[] args) {
        Hooks.enableAutomaticContextPropagation();
        SpringApplication.run(RinhaApplication.class, args);
    }

    @Bean
    public ReactiveCqlTemplate reactiveCqlTemplate(ReactiveSessionFactory sessionFactory) {
        return new ReactiveCqlTemplate(sessionFactory);
    }

    @Bean
    public RouterFunction<ServerResponse> functionalRoute(RinhaHandler handler, CqlSession session, ObjectMapper objectMapper) {
        return RouterFunctions.route(POST("/clientes/{accountId}/transacoes"), handler::handlePostRequest)
                .andRoute(GET("/clientes/{accountId}/extrato"), handler::handleGetRequest)
                .andRoute(GET("/health"), request -> healthEndpoint(session, objectMapper));
    }

    private Mono<ServerResponse> healthEndpoint(CqlSession session, ObjectMapper objectMapper) {
        try {
            Collection<Node> nodes = session.getMetadata().getNodes().values();
            Map<String, NodeState> response = Collections.singletonMap("status", nodes.stream().map(Node::getState).findFirst().orElse(NodeState.UNKNOWN));
            return ServerResponse.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(objectMapper.writeValueAsString(response));
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

}
