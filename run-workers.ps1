param(
    [string]$MasterHost,
    [string]$Port = '5000',
    [switch]$CheckOnly,
    [switch]$Detailed
)

# Called by run-workers.cmd. No installation or persistent environment changes.
$ErrorActionPreference = 'Stop'
try {
    $jarPath = Join-Path $PSScriptRoot 'distributed-kv.jar'
    if (-not (Test-Path -LiteralPath $jarPath -PathType Leaf)) {
        throw 'distributed-kv.jar is missing. Follow the build instructions in README.txt.'
    }

    $candidates = @()
    if ($env:JAVA_HOME) {
        $candidates += Join-Path $env:JAVA_HOME 'bin\java.exe'
    }
    $candidates += @(Get-Command java -CommandType Application -All -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty Source)
    $javaPath = $null
    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
        # Windows PowerShell treats native stderr as error records; java -version uses stderr.
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $versionOutput = (& $candidate -version 2>&1 | ForEach-Object { $_.ToString() }) -join "`n"
            $versionExitCode = $LASTEXITCODE
        } catch {
            continue
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        if ($versionExitCode -eq 0 -and
            $versionOutput -match '(?m)^(?:openjdk|java) version "(\d+)(?:[.\-"]|$)' -and
            [int]$Matches[1] -ge 17) {
            $javaPath = $candidate
            break
        }
        Write-Host "[INFO] Skipping unusable Java or version below 17: $candidate"
    }
    if (-not $javaPath) {
        throw 'Java 17+ was not found through JAVA_HOME or PATH. Install JDK 17, or set JAVA_HOME to its folder / add its bin folder to PATH. See README.txt (section 4).'
    }
    Write-Host "[OK] Java: $javaPath"
    Write-Host "[OK] JAR: $jarPath"
    if ($CheckOnly) { exit 0 }

    if ([string]::IsNullOrWhiteSpace($MasterHost)) {
        $MasterHost = Read-Host 'Master IP or hostname'
        $enteredPort = Read-Host 'Master port [5000]'
        if (-not [string]::IsNullOrWhiteSpace($enteredPort)) { $Port = $enteredPort }
    }
    $MasterHost = $MasterHost.Trim()
    if ([string]::IsNullOrWhiteSpace($MasterHost) -or $MasterHost -match '\s') {
        throw 'Enter a Master IP or hostname without spaces.'
    }
    $portNumber = 0
    if ($Port -notmatch '^\d{1,5}$' -or
        -not [int]::TryParse($Port, [ref]$portNumber) -or
        $portNumber -lt 1 -or $portNumber -gt 65535) {
        throw 'Master port must be an integer from 1 to 65535.'
    }

    # Stable log location even when launched from a different directory.
    Push-Location -LiteralPath $PSScriptRoot
    try {
        $workerArguments = @('-jar', $jarPath, 'workers', $MasterHost, "$portNumber")
        if ($Detailed) { $workerArguments += '--verbose' }
        & $javaPath @workerArguments
        $result = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    exit $result
} catch {
    Write-Host "[ERROR] $($_.Exception.Message)"
    exit 1
}
