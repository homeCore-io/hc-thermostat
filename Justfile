set shell := ["bash", "-uc"]

# Default: show recipes
default:
    @just --list

check: fmt clippy test

fmt:
    cargo fmt --all -- --check

fmt-fix:
    cargo fmt --all

clippy:
    cargo clippy --all-targets --no-deps -- -D warnings

test:
    cargo test --bin hc-thermostat

build:
    cargo build

build-release:
    cargo build --release

run:
    cargo run -- --config config/config.dev.toml

clean:
    cargo clean
