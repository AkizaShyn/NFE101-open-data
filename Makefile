ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
SERVICE_NAME=nfe101

# --project_name must be in lowercase
SERVICE_NAME_LOWER=$(shell echo "$(SERVICE_NAME)" | sed -e 's/\(.*\)/\1/')
# Git Folders
DOCKER_FOLDER=docker/


# Remote Folders
INSTALL_FOLDER=/secret/$(SERVICE_NAME)/

DOCKER_FILE=-f $(DOCKER_FOLDER)docker-compose.yml
DOCKER_COMMAND= $(ENV) docker compose -p $(SERVICE_NAME_LOWER) $(DOCKER_FILE)

start:
	$(DOCKER_COMMAND)  up -d --pull always

stop:
	$(DOCKER_COMMAND) down

remove:
	$(DOCKER_COMMAND) down -v

build:
	$(DOCKER_COMMAND) build

command:
	@echo $(DOCKER_COMMAND)

pull:
	$(DOCKER_COMMAND) pull

logs:
	$(DOCKER_COMMAND) logs -f --tail=100

