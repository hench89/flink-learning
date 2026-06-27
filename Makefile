.PHONY: help start stop restart logs clean submit-job kill-job job-status ui \
        kafka-topics kafka-consume db-connect db-query metrics checkpoints

WORKSHOP_DIR ?= $(HOME)/Repos/data-engineering-zoomcamp/07-streaming/workshop

help:
	@echo "Flink Learning - Commands"
	@echo ""
	@echo "Lifecycle:"
	@echo "  make start              Start infrastructure"
	@echo "  make stop               Stop all services"
	@echo "  make restart            Restart all services"
	@echo "  make logs [SVC=name]    Tail logs (jobmanager, taskmanager, redpanda, postgres)"
	@echo "  make clean              Remove containers and volumes"
	@echo ""
	@echo "Jobs:"
	@echo "  make submit-job JOB=path/to/job.py"
	@echo "  make kill-job JOB_ID=<id>"
	@echo "  make job-status         List running jobs"
	@echo ""
	@echo "Kafka:"
	@echo "  make kafka-topics"
	@echo "  make kafka-consume TOPIC=taxi-rides LINES=10"
	@echo ""
	@echo "Database:"
	@echo "  make db-connect         PostgreSQL shell"
	@echo "  make db-query SQL='...'"
	@echo ""
	@echo "Monitoring:"
	@echo "  make ui                 Open Flink Web UI"
	@echo "  make metrics JOB_ID=<id>"
	@echo "  make checkpoints JOB_ID=<id>"

start:
	cd $(WORKSHOP_DIR) && docker compose up -d
	@sleep 3
	@echo "Flink UI: http://localhost:8081"

stop:
	cd $(WORKSHOP_DIR) && docker compose down

restart: stop start

logs:
	cd $(WORKSHOP_DIR) && docker compose logs -f $(SVC)

clean:
	cd $(WORKSHOP_DIR) && docker compose down -v

submit-job:
	@test -n "$(JOB)" || (echo "Usage: make submit-job JOB=exercises/02-transformations/filter_rides.py" && exit 1)
	cd $(WORKSHOP_DIR) && docker compose exec jobmanager flink run -py /opt/flink/jobs/$(JOB)

kill-job:
	@test -n "$(JOB_ID)" || (echo "Usage: make kill-job JOB_ID=<job-id>" && exit 1)
	cd $(WORKSHOP_DIR) && docker compose exec jobmanager flink cancel $(JOB_ID)

job-status:
	cd $(WORKSHOP_DIR) && docker compose exec jobmanager flink list

ui:
	open http://localhost:8081 2>/dev/null || xdg-open http://localhost:8081

kafka-topics:
	cd $(WORKSHOP_DIR) && docker compose exec redpanda rpk topic list

kafka-consume:
	@test -n "$(TOPIC)" || (echo "Usage: make kafka-consume TOPIC=taxi-rides LINES=10" && exit 1)
	cd $(WORKSHOP_DIR) && docker compose exec redpanda rpk topic consume $(TOPIC) -n $(or $(LINES),10)

db-connect:
	cd $(WORKSHOP_DIR) && docker compose exec postgres psql -U postgres

db-query:
	@test -n "$(SQL)" || (echo "Usage: make db-query SQL='SELECT ...'" && exit 1)
	cd $(WORKSHOP_DIR) && docker compose exec postgres psql -U postgres -c "$(SQL)"

metrics:
	@test -n "$(JOB_ID)" || (echo "Usage: make metrics JOB_ID=<id>" && exit 1)
	@curl -s "http://localhost:8081/jobs/$(JOB_ID)/vertices" | python3 -m json.tool

checkpoints:
	@test -n "$(JOB_ID)" || (echo "Usage: make checkpoints JOB_ID=<id>" && exit 1)
	@curl -s "http://localhost:8081/jobs/$(JOB_ID)/checkpoints" | python3 -m json.tool
