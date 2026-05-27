# ============================================================================
# NOT COURSEWARE - COURSEWARE DEVELOPMENT INFRASTRUCTURE
# This script is part of the courseware development/CI infrastructure and is
# NOT part of the bootcamp course material. Students should ignore it.
# ============================================================================
#
# PowerShell counterpart to test_commands.sh.
# Runs the same dbt inline commands students copy from the course materials,
# but invokes dbt from PowerShell itself so we catch quoting / argument-
# forwarding regressions (apostrophes inside double-quoted args) that would
# never surface under Linux bash.

$script:Failed = 0
$script:Passed = 0

function Invoke-QuoteTest {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][scriptblock]$Block,
        [string]$ExpectInOutput
    )

    Write-Host "::group::Test: $Name"
    Write-Host "Running: $Block"
    $out = & $Block 2>&1 | Out-String
    Write-Host $out
    Write-Host "::endgroup::"

    if ($LASTEXITCODE -ne 0) {
        Write-Host "::error::FAILED: $Name (exit $LASTEXITCODE)"
        $script:Failed++
        return
    }
    if ($ExpectInOutput -and ($out -notmatch [regex]::Escape($ExpectInOutput))) {
        Write-Host "::error::FAILED: $Name (expected '$ExpectInOutput' not found in output)"
        $script:Failed++
        return
    }
    Write-Host "PASSED: $Name"
    $script:Passed++
}

Write-Host "=========================================="
Write-Host "Running dbt inline command tests (PowerShell: $($PSVersionTable.PSVersion))"
Write-Host "=========================================="

Invoke-QuoteTest "Jinja comments and variables" `
    { dbt compile --inline "{# This is a comment #}{% set my_name = 'Zoltan' %}{{ my_name }}" } `
    "Zoltan"

Invoke-QuoteTest "select_positive_values macro (compile)" `
    { dbt compile --inline "{{ select_positive_values('dim_listings_cleansed', 'price') }}" } `
    "WHERE price > 0"

Invoke-QuoteTest "select_positive_values macro (show)" `
    { dbt show --inline "{{ select_positive_values('dim_listings_cleansed', 'price') }}" } `
    "price"

# learn_logging emits "Call your mom!" via log() (file only) and
# "Call your dad!" via log(..., info=True) (stdout). Grep the one
# that actually reaches stdout.
Invoke-QuoteTest "learn_logging run-operation" `
    { dbt run-operation learn_logging } `
    "Call your dad!"

Invoke-QuoteTest "learn_variables run-operation" `
    { dbt run-operation learn_variables --vars "{user_name: zoltanctoth}" } `
    "zoltanctoth"

Invoke-QuoteTest "no_empty_strings macro (compile)" `
    { dbt compile --inline "SELECT * FROM {{ ref('dim_listings_cleansed') }} WHERE {{ no_empty_strings(ref('dim_listings_cleansed')) }}" } `
    "IS NOT NULL"

Invoke-QuoteTest "no_empty_strings macro (show)" `
    { dbt show --inline "SELECT * FROM {{ ref('dim_listings_cleansed') }} WHERE {{ no_empty_strings(ref('dim_listings_cleansed')) }}" } `
    $null

Write-Host "=========================================="
Write-Host "Test Summary: $($script:Passed) passed, $($script:Failed) failed"
Write-Host "=========================================="

if ($script:Failed -gt 0) {
    exit 1
}
Write-Host "All tests passed!"
