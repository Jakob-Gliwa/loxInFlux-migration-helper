# Static binary on scratch. Build: docker build -t loxinflux-migration-helper .
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev ca-certificates
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM scratch
# CA bundle for https connections to InfluxDB (the TLS verifier reads the system store)
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/target/release/loxinflux-migration-helper /loxinflux-migration-helper
WORKDIR /work
ENTRYPOINT ["/loxinflux-migration-helper"]
