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

exec "${TC_HOME}/tools/voter/bin/start-tc-voter.sh" "$@"
