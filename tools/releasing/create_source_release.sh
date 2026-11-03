#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
: "${RELEASE_VERSION:?Set the reviewed KES release version}"
[[ "$RELEASE_VERSION" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]] || { echo 'Invalid RELEASE_VERSION' >&2; exit 1; }
mkdir -p "$ROOT/release"
NAME="spark-kes-connector-${RELEASE_VERSION}"
OUT="$ROOT/release/${NAME}-src.tgz"
[[ ! -e "$OUT" ]] || { echo "Archive already exists: $OUT" >&2; exit 1; }
command -v gpg >/dev/null
if command -v sha512sum >/dev/null; then
    SHASUM=(sha512sum)
else
    SHASUM=(shasum -a 512)
fi
git -C "$ROOT" archive --format=tar.gz --prefix="${NAME}/" --output="$OUT" HEAD
gpg --armor --detach-sign "$OUT"
(cd "$ROOT/release" && "${SHASUM[@]}" "${NAME}-src.tgz" > "${NAME}-src.tgz.sha512")
printf 'Local KES source archive: %s\n' "$OUT"
