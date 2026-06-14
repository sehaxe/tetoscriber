#!/usr/bin/env bash
set -euo pipefail

sudo pacman -Syu --needed \
  base-devel \
  cargo \
  curl \
  docker \
  git \
  redis \
  rust \
  sqlite

if ! groups "$(whoami)" | grep -qw docker; then
  echo "Adding $USER to the docker group. Log out and back in for this to take effect."
  sudo usermod -aG docker "$USER"
fi

echo "Arch dependencies installed."
echo "Next steps:"
echo "  1. Install/configure NVIDIA driver and NVIDIA Container Toolkit for your kernel."
echo "  2. Reboot or log out/in if you changed the docker group."
echo "  3. Set NGC_API_KEY and run scripts/start-local.sh."
