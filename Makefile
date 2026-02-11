run-rule:
	cd services/rule-service && go run ./cmd/server

run-scheduler:
	cd services/scheduler-service && go run ./cmd/worker

test:
	cd services/rule-service && go test ./...
	cd services/scheduler-service && go test ./...

migrate:
	cd services/rule-service && go run ./cmd/migrate
