#!/usr/bin/env bash
# Installs Ubuntu packages on a GitHub-hosted runner.
#
# The runner image fetches the Ubuntu archive through the mirror list in
# /etc/apt/apt-mirrors.txt. When that mirror is unreachable, `apt-get update`
# only warns and the later package download fails. Treat any index failure as
# an error and retry with other official Ubuntu mirrors. apt still
# authenticates every index with the Ubuntu archive keyring, so a fallback
# mirror serves the same signed package versions.
set -euo pipefail

if (( $# == 0 )); then
  echo "Usage: $0 PACKAGE..." >&2
  exit 2
fi

mirror_list=/etc/apt/apt-mirrors.txt
fallbacks=(http://archive.ubuntu.com/ubuntu/ https://mirrors.edge.kernel.org/ubuntu/)
apt=(sudo apt-get -o Acquire::Retries=1)
install=(install -y --no-install-recommends "$@")

if "${apt[@]}" --error-on=any update && "${apt[@]}" "${install[@]}"; then
  exit 0
fi

if [[ ! -f $mirror_list ]]; then
  echo "::error::$mirror_list is missing; cannot switch Ubuntu mirrors." >&2
  exit 1
fi
for mirror in "${fallbacks[@]}"; do
  echo "::warning::Ubuntu packages were unavailable; retrying with $mirror"
  printf '%s\n' "$mirror" | sudo tee "$mirror_list" >/dev/null
  if "${apt[@]}" --error-on=any update && "${apt[@]}" "${install[@]}"; then
    exit 0
  fi
done

# A third-party repository can fail `--error-on=any` while Ubuntu indexes are
# current; installation from the last mirror decides the result.
"${apt[@]}" "${install[@]}"
