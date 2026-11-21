# Short commands for the project. Run from the project folder, e.g. "make run".

.PHONY: up down status run test-run logs

up:          ## start the cluster (10 machines)
	docker compose up -d

down:        ## stop the cluster (data is kept)
	docker compose down

status:      ## show which machines are running
	docker compose ps

run:         ## run the whole pipeline, start to finish
	./run_pipeline.sh

test-run:    ## quick run on 2 quarters only
	./run_pipeline.sh --test
