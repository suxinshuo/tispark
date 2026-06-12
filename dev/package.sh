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
#   dev/package.sh                 # packages the default profile (spark-3.5)
#   SPARK_PROFILE=spark-3.3 dev/package.sh
#   JAVA8_HOME=/path JAVA17_HOME=/path dev/package.sh
#
# Output: assembly/target/tispark-assembly-<release>_<scala>-<version>.jar
set -euo pipefail

SPARK_PROFILE="${SPARK_PROFILE:-spark-3.5}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
cd "$PROJECT_ROOT"

# --- locate the two JDKs (macOS: java_home; override via env on other OSes) ---
if [ -z "${JAVA8_HOME:-}" ] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA8_HOME="$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)"
fi
if [ -z "${JAVA17_HOME:-}" ] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
fi
if [ -z "${JAVA8_HOME:-}" ] || [ ! -x "$JAVA8_HOME/bin/java" ]; then
  echo "ERROR: Java 8 not found. Set JAVA8_HOME to a JDK 8 install." >&2; exit 1
fi
if [ -z "${JAVA17_HOME:-}" ] || [ ! -x "$JAVA17_HOME/bin/java" ]; then
  echo "ERROR: Java 17 not found. Set JAVA17_HOME to a JDK 17 install." >&2; exit 1
fi

# protoc 3.5.1 has no osx-aarch_64 build; if Java 8 is arm64, force the x86_64
# classifier (runs under Rosetta). Harmless on x86_64.
PROTOC_ARGS=()
if "$JAVA8_HOME/bin/java" -XshowSettings:properties -version 2>&1 | grep -q 'os.arch *= *aarch64'; then
  PROTOC_ARGS=(-Dos.detected.classifier=osx-x86_64 -Dos.detected.name=osx -Dos.detected.arch=x86_64)
fi

echo "==> Spark profile : $SPARK_PROFILE"
echo "==> Java 8  (tikv): $JAVA8_HOME"
echo "==> Java 17 (rest): $JAVA17_HOME"

# --- 0) generate TiSparkVersion.scala (version imprint) ---------------------
sh core/scripts/version.sh

# --- 1) tikv-client under Java 8, install into the local repo ---------------
echo "==> [1/2] building tikv-client (Java 8)"
JAVA_HOME="$JAVA8_HOME" mvn -pl tikv-client install -DskipTests "${PROTOC_ARGS[@]+"${PROTOC_ARGS[@]}"}"

# --- 2) core + wrappers + assembly under Java 17, against the Spark profile --
# Excludes tikv-client (built above under Java 8) and db-random-test (a standalone
# test-data framework not part of the assembly; its scaladoc doc-jar is unskippable
# and flaky under JDK 17).
echo "==> [2/2] packaging spark modules (Java 17, -P$SPARK_PROFILE)"
JAVA_HOME="$JAVA17_HOME" mvn -P"$SPARK_PROFILE" -pl '!tikv-client,!db-random-test' clean package \
  -DskipTests -DskipFetchTestData=true \
  -Dmaven.javadoc.skip=true -Dscalafmt.skip=true

echo "==> done. Assembly jar:"
ls -1 assembly/target/tispark-assembly-*.jar 2>/dev/null || true
