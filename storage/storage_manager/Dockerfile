FROM rust:latest

WORKDIR /app
COPY ./src ./src
COPY Cargo.toml .
RUN cargo build --release

CMD ["./target/release/storage_manager"]
