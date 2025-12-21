#!/usr/bin/env bash

set -euo pipefail


export SERVICE_CHECK_INTERVAL="${SERVICE_CHECK_INTERVAL:-2}"
export SERVICE_PRECONDITION="${SERVICE_PRECONDITION:-}"


main() {
    wait_for_preconditions
    load_entrypoint_hooks
    exec "$@"
}

load_entrypoint_hooks() {
    if [ -d /docker-entrypoint.d ]; then
        for f in $(find /docker-entrypoint.d -mindepth 1 -maxdepth 1 -type f -name '*.sh'| sort); do
            echo >&2 "$(date) INFO: sourcing $f"
            source "$f"
        done
    fi
}

wait_for_preconditions() (
    # Waits for a list of HOST:PORT pairs in $SERVICE_PRECONDITION to be accessible
    # This list can be comma- or space- separated

    if [ -z "$SERVICE_PRECONDITION" ]; then
        echo >&2 "$(date) INFO: no \$SERVICE_PRECONDITION"
        return 0
    fi

    eval set -- "${SERVICE_PRECONDITION//,/ }"
    wait_for_hosts "$@"
)

wait_for_host() {
    while ! test_tcp_connection "$@"; do
        echo >&2 "$(date) INFO: checking again in $SERVICE_CHECK_INTERVAL seconds"
        sleep "$SERVICE_CHECK_INTERVAL"
    done
}

wait_for_hosts() {
    for service in "$@"; do
        wait_for_host "$service"
    done
}

test_tcp_connection() {
    host="$1"
    port="${2:-}"

    if [ -z "$port" ]; then
        port="${host##*:}"
        host="${host%%:*}"
    fi

    if timeout 1 bash -c "< /dev/tcp/$host/$port" > /dev/null 2>&1; then
        echo >&2 "$(date) INFO: service $host:$port is accessible"
        return 0
    else
        echo >&2 "$(date) WARNING: service $host:$port is not accessible"
        return 1
    fi
}

current_monotonic_time() {
    # Return the current monotonic time in seconds using the /proc/uptime file
    awk '{print $1}' /proc/uptime | cut -d. -f1
}

if [ "$#" -gt 0 ]; then
    main "$@"
fi
