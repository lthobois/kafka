#!/usr/bin/env bash
# Prépare une VM Ubuntu pour les ateliers : Docker Engine, plugin Compose, images et dépendances.
# Usage, depuis la racine du dépôt cloné : sudo bash scripts/preparer-vm.sh
# Le script peut être relancé : il ne réinstalle pas ce qui est déjà présent.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Lancer avec sudo : sudo bash scripts/preparer-vm.sh" >&2
  exit 1
fi
UTILISATEUR="${SUDO_USER:-$USER}"
DEPOT="$(cd "$(dirname "$0")/.." && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "== Installation de Docker Engine depuis le dépôt officiel Docker"
  apt-get update -q
  apt-get install -y -q ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME:-$VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -q
  apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker >/dev/null

if ! id -nG "$UTILISATEUR" | grep -qw docker; then
  usermod -aG docker "$UTILISATEUR"
  echo "== $UTILISATEUR ajouté au groupe docker : se déconnecter puis se reconnecter en SSH"
fi

echo "== Préchargement des images et des dépendances"
cd "$DEPOT"
docker compose pull kafka
docker compose build app
docker compose version
echo "PREPARATION OK"
