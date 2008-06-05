#requires -version 1.0
###################################################################################################
## Run commands in multiple concurrent pipelines
##   by Arnoud Jansveld
## Version History
## 0.9    Includes logic to distinguish between scriptblocks and cmdlets or scripts. If a ScriptBlock
##        is specified, a foreach {} wrapper is added
## 0.8    Adds a progress bar
## 0.7    Stop adding runspaces if the queue is already empty
## 0.6    First version. Inspired by Gaurhoth's New-TaskPool script
###################################################################################################

function Split-Job (
    $Scriptblock = $(throw 'You must specify a command or script block!'),
    [int]$MaxPipelines=10
) {
    # Create the shared thread-safe queue and fill it with the input objects
    $Queue = [System.Collections.Queue]::Synchronized([System.Collections.Queue]@($Input))
    $QueueLength = $Queue.Count
    # Set up the script to be run by each pipeline
    if ($Scriptblock -is [ScriptBlock]) {$Scriptblock = "foreach {$Scriptblock}"}
    $Script = '$Queue = $($Input); & {while ($Queue.Count) {$Queue.Dequeue()}} | ' + $Scriptblock
    # Create an array to keep track of the set of pipelines
    $Pipelines = New-Object System.Collections.ArrayList

    function Add-Pipeline {
        # This creates a new runspace and starts an asynchronous pipeline with our script.
        # It will automatically start processing objects from the shared queue.
        $Runspace = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspace($Host)
        $Runspace.Open()
        $PipeLine = $Runspace.CreatePipeline($Script)
        $Null = $Pipeline.Input.Write($Queue)
        $Pipeline.Input.Close()
        $PipeLine.InvokeAsync()
        $Null = $Pipelines.Add($Pipeline)
    }

    function Remove-Pipeline ($Pipeline) {
        # Remove a pipeline and runspace when it is done
        $Pipeline.RunSpace.Close()
        $Pipeline.Dispose()
        $Pipelines.Remove($Pipeline)
    }

    # Start the pipelines
    do {Add-Pipeline} until ($Pipelines.Count -ge $MaxPipelines -or $Queue.Count -eq 0)

    # Loop through the pipelines and pass their output to the pipeline until they are finished
    while ($Pipelines.Count) {
        Write-Progress 'Split-Job' "Queues: $($Pipelines.Count)" `
            -PercentComplete (100 - [Int]($Queue.Count)/$QueueLength*100)
        foreach ($Pipeline in (New-Object System.Collections.ArrayList(,$Pipelines))) {
            if ( -not $Pipeline.Output.EndOfPipeline -or -not $Pipeline.Error.EndOfPipeline ) {
                $Pipeline.Output.NonBlockingRead()
                $Pipeline.Error.NonBlockingRead() | foreach {Write-Error $_}
            } else {
                if ($Pipeline.PipelineStateInfo.State -eq 'Failed') {
                    Write-Error $Pipeline.PipelineStateInfo.Reason
                }
                Remove-Pipeline $Pipeline
            }
        }
        Start-Sleep -Milliseconds 100
    }
}