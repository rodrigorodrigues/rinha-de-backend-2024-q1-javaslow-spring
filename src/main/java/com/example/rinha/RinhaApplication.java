package com.example.rinha;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.example.rinha.dto.TransactionRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.integration.hazelcast.lock.HazelcastLockRegistry;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;
import org.springframework.web.server.WebFilter;
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
        SpringApplication.run(RinhaApplication.class, args);
    }

    @Bean
    public ReactiveCqlTemplate reactiveCqlTemplate(ReactiveSessionFactory sessionFactory) {
        return new ReactiveCqlTemplate(sessionFactory);
    }

    @Bean
    public HazelcastInstance hazelcastInstance() {
        return Hazelcast.newHazelcastInstance();
    }

    @Bean
    public LockRegistry lockRegistry() {
        return new HazelcastLockRegistry(hazelcastInstance());
    }

    @Bean
    public RouterFunction<ServerResponse> routes(RinhaHandler handler, CqlSession session, ObjectMapper objectMapper) {
        return RouterFunctions.route(POST("/clientes/{accountId}/transacoes"), handler::handlePostRequest)
                .andRoute(GET("/clientes/{accountId}/extrato"), handler::handleGetRequest)
                .andRoute(GET("/health"), request -> healthEndpoint(session, objectMapper));
    }

    @Bean
    public WebFilter mappingErrorToUnprocessableEntity() {
        return (exchange, next) -> next.filter(exchange)
                .onErrorResume(ServerWebInputException.class, e -> {
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(HttpStatus.UNPROCESSABLE_ENTITY);
                    return response.setComplete();
                })
                .onErrorResume(ResponseStatusException.class, e -> {
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(e.getStatusCode());
                    return response.setComplete();
                });
    }

    @Bean
    Jackson2ObjectMapperBuilderCustomizer loomCustomizer() {
        var jsonFactory = JsonFactory.builder().recyclerPool(JsonRecyclerPools.sharedLockFreePool()).build();
        return builder -> builder.factory(jsonFactory);
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
