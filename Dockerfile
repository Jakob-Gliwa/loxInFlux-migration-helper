# Static binary on scratch. Build: docker build -t loxinflux-migrationshelper .
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/loxinflux-migrationshelper /loxinflux-migrationshelper
WORKDIR /work
ENTRYPOINT ["/loxinflux-migrationshelper"]
