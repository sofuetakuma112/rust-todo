build:
	docker compose build

db:
	docker compose up

dev:
	sqlx database drop -y
	sqlx db create
	sqlx migrate run
	cargo watch -x run

test:
	sqlx database drop -y
	sqlx db create
	sqlx migrate run
	cargo test

# standalone test(DB接続が発生しないテストのみ実行)
test-s:
	cargo test --no-default-features