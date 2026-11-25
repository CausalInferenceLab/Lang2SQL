#!/usr/bin/env bash
set -euo pipefail

transport=(--transport litellm:acompletion)
if [[ ${1:-} == "--smoke" ]]; then
    transport=()
    shift
fi
if [[ $# -ne 6 ]]; then
    echo "Usage: matrix.sh [--smoke] PRODUCT_TREE PRODUCT_SHA PREPARED SOURCE DB_DIR OUTPUT" >&2
    exit 2
fi
product_tree=$(realpath "$1")
product_sha=$2
prepared=$(realpath "$3")
source=$(realpath "$4")
db_dir=$(realpath "$5")
output=$(realpath -m "$6")
gpt41_python=${BIRD_GPT41_PY:?Set BIRD_GPT41_PY to the absolute path of the gpt41 interpreter}
luna_python=${BIRD_LUNA_PY:?Set BIRD_LUNA_PY to the absolute path of the luna interpreter}

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
mkdir -p "$output"

run_profile() (
    local profile=$1
    local python=$2
    local condition
    for condition in B D; do
        local lane="$output/$condition-$profile"
        local resume=()
        if [[ -d "$lane" ]]; then
            resume=(--resume)
        fi
        printf '%s\n' "Running $condition-$profile"
        "$python" -m bench.bird.evaluate run \
            --prepared "$prepared" --db-dir "$db_dir" --output "$lane" \
            --condition "$condition" --profile "$profile" \
            --product-tree "$product_tree" --product-commit "$product_sha" \
            "${transport[@]}" "${resume[@]}" > "$lane.run.log"
        "$python" -m bench.bird.evaluate score \
            --source "$source" --db-dir "$db_dir" --output "$lane" > "$lane.score.log"
        printf '%s\n' "Scored $condition-$profile"
    done
)

run_profile openai-gpt41-mini "$gpt41_python" &
mini_pid=$!
run_profile openai-gpt41 "$gpt41_python" &
full_pid=$!
run_profile openai-gpt6-luna "$luna_python" &
luna_pid=$!

result=0
for pid in "$mini_pid" "$full_pid" "$luna_pid"; do
    wait "$pid" || result=1
done
exit "$result"
