#!/bin/bash

TC_HOME="/terracotta"
CFG_DIR="${TC_HOME}/config"     # Read-Only !
RUN_DIR="${TC_HOME}/run"        # Read-Write

set -e

# Forcing HOME, USER, user.home and user.name.
# This is required to put back the right values in the case containers are running with random uid:0.
# In this case:
# - HOME is set to / (unknown uid)
# - USER is not set
# - user.home and user.name are not properly defined in JVM and leads to ? (unknown uid)
# So this ensures programs will run fine enclosed within $TC_HOME.
# Additional note: $TC_HOME is installed with chmod 774.
# All folders inside $SAG_HOME requiring some manipulations (write) will also need the same type of
# chmod so that any user from group 0 can perform these manipulations.
id -un >/dev/null 2>&1 && UNKNOWN="" || UNKNOWN="user-$(id -u)"
if [ ! -z "$UNKNOWN" ]; then
  echo "Running as unknown user ($UNKNOWN)..."
  echo " - Enforcing HOME=${TC_HOME}"
  echo " - Enforcing USER=${UNKNOWN}"
  export HOME="${TC_HOME}"
  export USER="${UNKNOWN}"
  export JAVA_OPTS="${JAVA_OPTS} -Duser.home=${TC_HOME} -Duser.name=${UNKNOWN}"
fi

# VALIDATION RW ON RUN_DIR
mkdir -p "${RUN_DIR}"
touch "${RUN_DIR}/qwerty" && rm "${RUN_DIR}/qwerty"
[ ! $? -eq 0 ] && echo "Unable to write into ${RUN_DIR}" && exit 2

CONFIG_DIR="${RUN_DIR}/config"
LOG_DIR="${RUN_DIR}/logs"

# Create folders in run directory.
# This directory can optionally be mounted and optionally be used
# It is important to keep them as-is even if unused (i.e. log-dir)
# because we document to use the run folder with these folders inside.
# Plus, when running in OpenShift with a random UID, this UID will have
# The permissions to write and create directories into RUN_DIR but
# not in TC_HOME. TC_HOME is a read-only location for these random UID,
# this is done for security purposes and to ensure container immutability.
mkdir -p "${CONFIG_DIR}"
mkdir -p "${LOG_DIR}"

# STDOUT logging
# All containers should log to STDOUT and not file by default (a file would slow down the container and grow its FS)
# It is still possible to log to a file by providing a value to log-dir
# Explanation of how log-dir interacts in dynamic config: https://github.com/Terracotta-OSS/terracotta-platform/pull/1109
export JAVA_OPTS="${JAVA_OPTS} -Dterracotta.config.logDir.noDefault=true"

if [ $# -eq 0 ]; then

  # determine hostname in a reliable way for docker and kube
  # hostname will be used both for server name and hostname
  NAME="$(hostname -f)"

  # auto activate ?
  ACTIVATE=""
  if [ "${DEFAULT_ACTIVATE}" == "true" ]; then
    ACTIVATE="-auto-activate"
  fi

  # Default parameters to use when none is specified
  set -x
  exec "${TC_HOME}/server/bin/start-tc-server.sh" \
    -config-dir "${CONFIG_DIR}" \
    -failover-priority "${DEFAULT_FAILOVER}" \
    -offheap-resources "${DEFAULT_OFFHEAP}" \
    -name "${NAME}" \
    -hostname "${NAME}" \
    -cluster-name "${DEFAULT_CLUSTER_NAME}" ${ACTIVATE}

else

  set -x
  exec "${TC_HOME}/server/bin/start-tc-server.sh" "$@"

fi
