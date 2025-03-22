# Run the command 5 times and store the TotalMilliseconds for each run.
$times = 1..5 | ForEach-Object {
    $time = Measure-Command { python main.py }
    $time.TotalMilliseconds
}

# Compute the average time.
$average = ($times | Measure-Object -Average).Average

# Output each run's time and the average.
$times | ForEach-Object -Begin { $i = 1 } -Process {
    Write-Output "Run $i took $_ ms"
    $i++
}
Write-Output "Average time: $average ms"
