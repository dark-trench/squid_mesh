#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow_path="$repo_root/.github/workflows/discord-notifications.yml"
tmp_dir="$(mktemp -d)"

trap 'rm -rf "$tmp_dir"' EXIT

script_path="$tmp_dir/notify.sh"
awk '
  /^        run: \|$/ {
    in_run = 1
    next
  }

  in_run {
    if ($0 ~ /^          /) {
      sub(/^          /, "")
      print
      next
    }

    if ($0 == "") {
      print ""
      next
    }

    exit
  }
' "$workflow_path" > "$script_path"

stub_bin="$tmp_dir/bin"
mkdir -p "$stub_bin"

cat > "$stub_bin/curl" <<'CURL'
#!/usr/bin/env bash
set -euo pipefail

payload=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -d)
      payload="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s' "$payload" > "$CURL_PAYLOAD_PATH"
CURL

chmod +x "$stub_bin/curl"

run_notification() {
  local event_name="$1"
  local event_json="$2"
  local event_path="$tmp_dir/$event_name-event.json"
  local payload_path="$tmp_dir/$event_name-payload.json"

  printf '%s' "$event_json" > "$event_path"

  PATH="$stub_bin:$PATH" \
    DISCORD_WEBHOOK="https://discord.example/webhook" \
    ACTOR="octocat" \
    EVENT_NAME="$event_name" \
    REPO="dark-trench/example" \
    GITHUB_EVENT_PATH="$event_path" \
    CURL_PAYLOAD_PATH="$payload_path" \
    bash "$script_path"

  jq -r '.content' "$payload_path"
}

assert_multiline_content() {
  local content="$1"
  local label="$2"

  if [[ "$content" == *'\n'* ]]; then
    printf 'expected %s to contain real newlines, got literal \\n: %s\n' "$label" "$content" >&2
    exit 1
  fi

  if [[ "$content" != *$'\n'* ]]; then
    printf 'expected %s to contain at least one newline: %s\n' "$label" "$content" >&2
    exit 1
  fi
}

watch_content="$(run_notification watch '{}')"
assert_multiline_content "$watch_content" "watch notification"

issue_content="$(
  run_notification issues '{
    "issue": {
      "title": "issue title",
      "html_url": "https://github.com/dark-trench/example/issues/1"
    }
  }'
)"
assert_multiline_content "$issue_content" "issue notification"
