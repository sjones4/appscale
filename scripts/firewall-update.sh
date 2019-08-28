#!/bin/bash

# Remove temporary files on exit
cleanup() {
  [ ! -f "${IPTABLES_FILE:-}" ] || rm -f "${IPTABLES_FILE}"
}

trap cleanup EXIT

# Output rules to file
iptables() {
  if [ "-F" == "${1}" ] ; then
    return # drop flush, we will replace all rules
  fi
  if [ -z "${IPTABLES_FILE:-}" ] ; then
    IPTABLES_FILE="$(mktemp --tmpdir=/run/appscale/ iptables.XXXXXXXXXX)"
    echo "*filter"               >  "${IPTABLES_FILE}"
    echo ":INPUT ACCEPT [0:0]"   >> "${IPTABLES_FILE}"
    echo ":FORWARD ACCEPT [0:0]" >> "${IPTABLES_FILE}"
    echo ":OUTPUT ACCEPT [0:0]"  >> "${IPTABLES_FILE}"
  fi
  echo "$*" >> "${IPTABLES_FILE}"
}

# Update firewall atomically
update_firewall() {
  echo "COMMIT" >> "${IPTABLES_FILE}"

  if [ -f "/run/appscale/iptables.applied" ] ; then
    if diff --brief "/run/appscale/iptables.applied" "${IPTABLES_FILE}" &>/dev/null; then
      rm "${IPTABLES_FILE}"
    fi
  fi

  if [ -f "${IPTABLES_FILE}" ] ; then
    if iptables-restore --table filter "${IPTABLES_FILE}"; then
      mv -f "${IPTABLES_FILE}" "/run/appscale/iptables.applied"
    else
      mv -f "${IPTABLES_FILE}" "/run/appscale/iptables.failed"
    fi
  fi
}

# Source and process firewall configuration
if [ -f "${1:-}" ] ; then
  source ${1}
  update_firewall
fi

