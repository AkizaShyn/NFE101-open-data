# NFE101-open-data

# Prérequis 
- Avoir docker sur son poste et docker compose voir la documentation officielle pour l'installation https://docs.docker.com/get-started/

# Installation

```bash
# Clonage du projet
git clone git@github.com:AkizaShyn/NFE101-open-data.git

# Installer make si vous n'avez pas la commande dispo
# Sur debian / ubuntu
sudo apt update -y && sudo apt install make -y

# sur mac
brew install make

# Build de la stack
sudo make build 

# Lancement de la stack
sudo make start

# Voir les logs de la stack
sudo make logs
```

# Lister les topic 
```bash
make list_topic
```

# insérer une donnée 
```bash
# entrer dans le shell de kafka
make message_shell
# Puis coller le message que l'on souhaite envoyer via le data/messages.jsonl
```