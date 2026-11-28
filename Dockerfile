# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hamilton-server -p hamilton-cli \
    && cp target/release/hamilton-server target/release/hamilton-cli /usr/local/bin/

FROM debian:bookworm-slim
COPY --from=build /usr/local/bin/hamilton-server /usr/local/bin/hamilton-cli /usr/local/bin/
WORKDIR /data
VOLUME /data
EXPOSE 6942
STOPSIGNAL SIGTERM
ENTRYPOINT ["hamilton-server", "--host", "0.0.0.0", "--port", "6942"]
CMD ["/data/hamilton.db"]
