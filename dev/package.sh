#!/usr/bin/env bash
#
#   Copyright 2026 PingCAP, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# Package TiSpark for a given Spark version.
#
# WHY THIS SCRIPT EXISTS: the build needs TWO JDKs and cannot run as a single
# `mvn package` over the whole reactor:
#   * tikv-client must compile under Java 8 (uses sun.misc.Cleaner and gRPC code
#     that references javax.annotation.Generated, both removed in Java 9+).
#   * core / spark-wrapper / assembly must compile under Java 17 (Spark 3.3+).
# So we build & install tikv-client with Java 8 first, then build everything
# else with Java 17 against the chosen Spark profile.
#
# Usage:
#   dev/package.sh -p spark-3.5        # package for Spark 3.5
#   dev/package.sh -p spark-3.3        # package for Spark 3.3
#   dev/package.sh                     # defaults to spark-3.5
#   dev/package.sh -h                  # show help
#
# Spark 3.5+ uses Java 17 for core/spark-wrapper/assembly;
# Spark 3.0-3.3 uses Java 8 for everything.
#
# Output: assembly/target/tispark-assembly-<release>_<scala>-<version>.jar
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
cd "$PROJECT_ROOT"

# ---------------------------------------------------------------------------
# help
# ---------------------------------------------------------------------------
usage() {
  cat <<'EOF'
Usage: dev/package.sh [OPTIONS] [SPARK_VERSION]

Package TiSpark for a given Spark version.

Options:
  -p, --profile PROFILE   Maven profile name (e.g. spark-3.3, spark-3.5)
  -h, --help              Show this help message

You can also pass a bare version number as a positional argument:
  dev/package.sh 3.5       same as -p spark-3.5

Or set the SPARK_PROFILE environment variable:
  SPARK_PROFILE=spark-3.3 dev/package.sh

Environment variables:
  JAVA8_HOME              Path to JDK 8 (auto-detected on macOS)
  JAVA17_HOME             Path to JDK 17 (auto-detected on macOS)

Versions:
  spark-3.0, spark-3.1, spark-3.2, spark-3.3   →  Java 8 only
  spark-3.5                                     →  Java 8 (tikv) + Java 17 (rest)

Output:
  assembly/target/tispark-assembly-<release>_<scala>-<version>.jar
EOF
  exit 0
}

# ---------------------------------------------------------------------------
# parse arguments
# ---------------------------------------------------------------------------
SPARK_VERSION_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile) SPARK_PROFILE="$2"; shift 2 ;;
    -h|--help)    usage ;;
    --*)          echo "Unknown option: $1" >&2; usage ;;
    -*)           echo "Unknown option: $1" >&2; usage ;;
    *)            SPARK_VERSION_ARG="$1"; shift ;;
  esac
done

# positional version number (e.g. "3.5") → profile name
if [ -n "${SPARK_VERSION_ARG:-}" ]; then
  SPARK_PROFILE="spark-${SPARK_VERSION_ARG}"
fi

# fallback: env var or default
SPARK_PROFILE="${SPARK_PROFILE:-spark-3.5}"

# ---------------------------------------------------------------------------
# determine which JDK to use for core/spark-wrapper/assembly
# ---------------------------------------------------------------------------
# Extract the minor version: "spark-3.5" → "3.5", "spark-3.3" → "3.3"
SPARK_VERSION_NUM="${SPARK_PROFILE#spark-}"

# Compare version: 3.5+ needs Java 17, earlier needs Java 8
need_java17_for_core() {
  local major minor
  IFS=. read -r major minor <<<"$SPARK_VERSION_NUM"
  # Spark 3.5+ (major=3, minor>=5) or any 4.x+
  if [ "$major" -gt 3 ] || { [ "$major" -eq 3 ] && [ "${minor:-0}" -ge 5 ]; }; then
    return 0
  fi
  return 1
}

# ---------------------------------------------------------------------------
# locate JDKs
# ---------------------------------------------------------------------------
find_java_home() {
  local ver="$1"
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    /usr/libexec/java_home -v "$ver" 2>/dev/null || true
  fi
}

JAVA8_HOME="${JAVA8_HOME:-$(find_java_home 1.8)}"
if [ -z "${JAVA8_HOME:-}" ] || [ ! -x "$JAVA8_HOME/bin/java" ]; then
  echo "ERROR: Java 8 not found. Set JAVA8_HOME to a JDK 8 install." >&2; exit 1
fi

if need_java17_for_core; then
  JAVA17_HOME="${JAVA17_HOME:-$(find_java_home 17)}"
  if [ -z "${JAVA17_HOME:-}" ] || [ ! -x "$JAVA17_HOME/bin/java" ]; then
    echo "ERROR: Java 17 not found. Set JAVA17_HOME to a JDK 17 install." >&2; exit 1
  fi
  CORE_JDK="$JAVA17_HOME"
  CORE_JDK_LABEL="Java 17"
else
  CORE_JDK="$JAVA8_HOME"
  CORE_JDK_LABEL="Java 8"
fi

# ---------------------------------------------------------------------------
# protoc args for arm64 Mac
# ---------------------------------------------------------------------------
PROTOC_ARGS=()
if "$JAVA8_HOME/bin/java" -XshowSettings:properties -version 2>&1 | grep -q 'os.arch *= *aarch64'; then
  PROTOC_ARGS=(-Dos.detected.classifier=osx-x86_64 -Dos.detected.name=osx -Dos.detected.arch=x86_64)
fi

# ---------------------------------------------------------------------------
# print summary
# ---------------------------------------------------------------------------
echo "==> Spark profile : $SPARK_PROFILE"
echo "==> Java 8 (tikv+db-random-test): $JAVA8_HOME"
echo "==> $CORE_JDK_LABEL (rest): $CORE_JDK"

# --- 0) generate TiSparkVersion.scala (version imprint) ---------------------
sh core/scripts/version.sh

# --- 1) tikv-client + db-random-test under Java 8, install into local repo --
echo "==> [1/2] building tikv-client, db-random-test (Java 8)"
JAVA_HOME="$JAVA8_HOME" mvn -pl tikv-client,db-random-test install -DskipTests \
  -Dmaven.javadoc.skip=true "${PROTOC_ARGS[@]+"${PROTOC_ARGS[@]}"}"

# --- 2) core + wrappers + assembly under the appropriate JDK ---------------
echo "==> [2/2] packaging spark modules ($CORE_JDK_LABEL, -P$SPARK_PROFILE)"
JAVA_HOME="$CORE_JDK" mvn -P"$SPARK_PROFILE" -pl '!tikv-client,!db-random-test' clean package \
  -DskipTests -DskipFetchTestData=true \
  -Dmaven.javadoc.skip=true -Dscalafmt.skip=true

echo "==> done. Assembly jar:"
ls -1 assembly/target/tispark-assembly-*.jar 2>/dev/null || true