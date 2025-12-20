export USER_UID ?= $(shell id -u)
export USER_GID ?= $(shell id -g)

.PHONY: dev/sync
dev/sync:
	uv sync --all--groups

.PHONY: dev/build
dev/build:
	docker compose --progress=plain build

.PHONY: dev/up
dev/up:
	docker compose --progress=plain up --build -d
	docker compose logs -f

.PHONY: dev/logs
dev/logs:
	docker compose logs -f

.PHONY: dev/rm
dev/rm:
	docker-compose kill --signal SIGKILL || true
	docker-compose stop --timeout 5 || true
	docker-compose rm -fsv
	docker-compose down --remove-orphans

.PHONY: build
build:
	@rm -rf dist/
	@python3 setup.py bdist_wheel

.PHONY: publish
publish: build
	@bash -c "read -r -p 'Really publish (y/N)? ' response && [[ \$${response,,} == 'y' ]]"
	@twine upload dist/*
