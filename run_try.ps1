# PowerShell script to run try.py with SSL verification bypassed
$env:SEC_API_VERIFY_SSL = "false"
py try.py

