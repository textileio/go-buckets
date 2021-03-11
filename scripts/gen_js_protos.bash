#!/bin/bash
set -eo pipefail

wd="$(pwd -P)"

js_paths=()
while IFS=  read -r -d $'\0'; do
  js_paths+=("$REPLY")
done < <(find . -path "*/pb/javascript" ! -path "*/node_modules/*" -print0)

echo installing dependencies
for path in "${js_paths[@]}"; do
  cd "${path}" && npm install >/dev/null 2>&1 && cd "${wd}"
done

echo generating js-protos in api/pb/buckets/javascript
./buildtools/protoc/bin/protoc \
  --proto_path=. \
  --plugin=protoc-gen-ts=api/pb/buckets/javascript/node_modules/.bin/protoc-gen-ts \
  --js_out=import_style=commonjs,binary:api/pb/buckets/javascript \
  --ts_out=service=grpc-web:api/pb/buckets/javascript \
  api/pb/buckets/buckets.proto
