#!/usr/bin/env bash
# Can processes inside WSL reach the Docker Desktop containers?
gw=$(ip route | awk '/default/ {print $3}')
for host in 127.0.0.1 "$gw"; do
  for port in 27017 9092 9000; do
    if timeout 3 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then r=OK; else r=unreachable; fi
    echo "$host:$port $r"
  done
done
