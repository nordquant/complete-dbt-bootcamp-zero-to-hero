#!/bin/bash

# Usage: test_commands.sh [core|fusion]
# Default: core
ENGINE="${1:-core}"

# Track overall success
FAILED=0
PASSED=0

run_test() {
    local name="$1"
    shift
    echo "::group::Test: $name"
    echo "Running: $@"
    if "$@"; then
        echo "::endgroup::"
        echo "PASSED: $name"
        ((PASSED++))
    else
        echo "::endgroup::"
        echo "::error::FAILED: $name"
        ((FAILED++))
    fi
}

echo "=========================================="
echo "Running dbt inline command tests (engine: $ENGINE)"
echo "=========================================="

# Core-only tests (not compatible with dbt Fusion)
if [ "$ENGINE" = "core" ]; then
    run_test "Jinja comments and variables" \
        dbt compile --inline "{# This is a comment #}{% set my_name = 'Zoltan' %}{{ my_name }}"
fi

# Tests that run on both Core and Fusion
run_test "select_positive_values macro (compile)" \
    dbt compile --inline "{{ select_positive_values('dim_listings_cleansed', 'price') }}"

run_test "select_positive_values macro (show)" \
    dbt show --inline "{{ select_positive_values('dim_listings_cleansed', 'price') }}"

run_test "learn_logging run-operation" \
    dbt run-operation learn_logging

run_test "learn_variables run-operation" \
    dbt run-operation learn_variables --vars "{user_name: zoltanctoth}"

run_test "no_empty_strings macro (compile)" \
    dbt compile --inline "SELECT * FROM {{ ref('dim_listings_cleansed') }} WHERE {{ no_empty_strings(ref('dim_listings_cleansed')) }}"

run_test "no_empty_strings macro (show)" \
    dbt show --inline "SELECT * FROM {{ ref('dim_listings_cleansed') }} WHERE {{ no_empty_strings(ref('dim_listings_cleansed')) }}"

echo "=========================================="
echo "Test Summary: $PASSED passed, $FAILED failed"
echo "=========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi
echo "All tests passed!"
