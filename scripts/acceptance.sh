#!/usr/bin/env bash

set -euo pipefail

mode="${1:-all}"

run_test() {
    local test_name="$1"
    local filter="$2"
    cargo test --test "$test_name" "$filter" -- --ignored --nocapture
}

run_network() {
    run_test eastmoney_funds eastmoney_fund_rules_fetch_ranking_and_overview
    run_test eastmoney_funds real_http_keyword_post_round_trip
    run_test json_selector eastmoney_json_selector_smoke
}

run_browser() {
    if [[ -z "${WEBDRIVER_URL:-}" ]]; then
        echo "skip browser acceptance: WEBDRIVER_URL is not set"
        return
    fi
    run_test eastmoney_funds eastmoney_fund_rules_run_full_graph
    run_test x_profile x_profile_rules_fetch_posts_and_items
}

run_x_account() {
    if [[ -z "${X_USERNAME:-}" || -z "${X_BEARER_TOKEN:-}" ]]; then
        echo "skip X account acceptance: X_USERNAME and X_BEARER_TOKEN are not set"
        return
    fi
    run_test x_account x_account_rules_fetch_real_posts_and_follow_next_token
}

case "$mode" in
    network)
        run_network
        ;;
    browser)
        run_browser
        ;;
    x-account)
        run_x_account
        ;;
    all)
        run_network
        run_browser
        run_x_account
        ;;
    *)
        printf 'usage: %s [all|network|browser|x-account]\n' "$0" >&2
        exit 2
        ;;
esac
