setup:
	pip install -r dev-requirements.txt
	docker compose -f ./integration_tests/docker-compose.yml up -d

teardown:
	docker compose -f ./integration_tests/docker-compose.yml down
