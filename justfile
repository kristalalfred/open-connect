mod bench

default:
    @just --list

build:
    cargo build

test:
    cargo test --workspace
    just browser-check

fmt:
    cargo fmt

fmt-check:
    cargo fmt --all --check

contracts:
    cargo run -p connect-core --bin generate-contracts

contracts-check:
    cargo test -p connect-core contracts::tests

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

browser-check:
    node nodes/browser/check.mjs --check-only
    node --test crates/controller/src/ui.test.mjs

run-controller:
    cargo run -p connect-controller

run-strom-adapter *ARGS:
    cargo run -p connect-adapter-strom -- {{ARGS}}

run-node *ARGS:
    cargo run -p connect-media --bin connect-node -- {{ARGS}}

timed-demo ACTION="run" *ARGS: build
    cargo run -p connect-bench --bin connect-timed-demo -- {{ACTION}} {{ARGS}}

cli *ARGS:
    cargo run -p connect-cli -- {{ARGS}}

apply FILE="examples/stream.yaml":
    cargo run -p connect-cli -- apply -f {{FILE}}

apply-set OWNER FILE="examples/stream-set.yaml":
    cargo run -p connect-cli -- apply-set {{OWNER}} -f {{FILE}}

plan FILE="examples/stream.yaml":
    cargo run -p connect-cli -- plan -f {{FILE}}

get-streams:
    cargo run -p connect-cli -- get streams

get-stream NAME:
    cargo run -p connect-cli -- get stream {{NAME}}

get-nodes:
    cargo run -p connect-cli -- get nodes

get-status:
    cargo run -p connect-cli -- get status

get-endpoints NAME:
    cargo run -p connect-cli -- get endpoints {{NAME}}

delete-stream NAME:
    cargo run -p connect-cli -- delete stream {{NAME}}
