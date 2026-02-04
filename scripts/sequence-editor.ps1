param($todo)

# Replace 'pick <hash>' for the target commit with 'reword <hash>'
(Get-Content -Raw -LiteralPath $todo) -replace 'pick 0b7d4a0','reword 0b7d4a0' | Set-Content -LiteralPath $todo
