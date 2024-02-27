FROM container-registry.oracle.com/os/oraclelinux:8-slim
COPY target/rinha-backend-2024q1-javaslow-spring javaslow-spring
ENTRYPOINT ["/javaslow-spring"]

