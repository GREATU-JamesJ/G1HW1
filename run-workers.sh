#!/usr/bin/env bash
# Use: bash run-workers.sh [MASTER_HOST [PORT]] or bash run-workers.sh --check
set -u

fail() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }
script_parent=${BASH_SOURCE[0]%/*}
[[ "$script_parent" != "${BASH_SOURCE[0]}" ]] || script_parent=.
script_dir=$(CDPATH= cd -- "$script_parent" && pwd) || exit 1
jar_path="$script_dir/distributed-kv.jar"
[[ -f "$jar_path" ]] || fail 'distributed-kv.jar is missing. See the build instructions in README.txt.'

candidates=()
[[ -z "${JAVA_HOME:-}" ]] || candidates+=("$JAVA_HOME/bin/java")
path_java=$(type -P java || true)
[[ -z "$path_java" ]] || candidates+=("$path_java")
java_path=''
version_pattern='^(openjdk|java) version "([0-9]+)[.\"-]'
for candidate in "${candidates[@]}"; do
    [[ -x "$candidate" ]] || continue
    if version_output=$("$candidate" -version 2>&1); then
        while IFS= read -r version_line; do
            if [[ "$version_line" =~ $version_pattern ]] && (( BASH_REMATCH[2] >= 17 )); then
                java_path=$candidate
                break
            fi
        done <<< "$version_output"
    fi
    [[ -z "$java_path" ]] || break
    printf '[INFO] Skipping unusable Java or version below 17: %s\n' "$candidate"
done
[[ -n "$java_path" ]] || fail 'Java 17+ was not found through JAVA_HOME or PATH. Install JDK 17, or set JAVA_HOME / add its bin folder to PATH. See README.txt (section 4).'
printf '[OK] Java: %s\n[OK] JAR: %s\n' "$java_path" "$jar_path"

if [[ "${1:-}" == '--check' && $# -eq 1 ]]; then exit 0; fi
verbose_args=()
positional=()
for argument in "$@"; do
    case "$argument" in
        --verbose) verbose_args=(--verbose) ;;
        --*) fail "Unknown option: $argument" ;;
        *) positional+=("$argument") ;;
    esac
done
set -- "${positional[@]}"
(( $# <= 2 )) || fail 'Usage: bash run-workers.sh [MASTER_HOST [PORT]] or --check'
master_host=${1:-}
port=${2:-5000}
if (( $# == 0 )); then
    read -r -p 'Master IP or hostname: ' master_host || fail 'No Master address supplied.'
    read -r -p 'Master port [5000]: ' port || fail 'No port input supplied.'
    port=${port:-5000}
fi
[[ -n "$master_host" && "$master_host" != '--check' && ! "$master_host" =~ [[:space:]] ]] || fail 'Enter a Master IP or hostname without spaces.'
[[ "$port" =~ ^[0-9]{1,5}$ ]] || fail 'Master port must be an integer from 1 to 65535.'
port=$((10#$port))
(( port >= 1 && port <= 65535 )) || fail 'Master port must be an integer from 1 to 65535.'
cd -- "$script_dir" || exit 1
exec "$java_path" -jar "$jar_path" workers "$master_host" "$port" "${verbose_args[@]}"
