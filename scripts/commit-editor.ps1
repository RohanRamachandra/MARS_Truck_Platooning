param($msgFile)

# Overwrite the commit message with the desired text
Set-Content -LiteralPath $msgFile -Value "Initial commit"
