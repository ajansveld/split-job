#requires -version 1.0
################################################################################
## Run commands in multiple concurrent pipelines
##   by Arnoud Jansveld - www.jansveld.net/powershell
##
## Basic "drop in" usage examples:
##   - Functions that accept pipelined input:
##       Without Split-Job:
##          Get-Content hosts.txt | MyFunction | Export-Csv results.csv
##       With Split-Job:
##          Get-Content hosts.txt | Split-Job {MyFunction} | Export-Csv results.csv
##   - Functions that do not accept pipelined input (use foreach):
##       Without Split-Job:
##          Get-Content hosts.txt |% { .\MyScript.ps1 -ComputerName $_ } | Export-Csv results.csv
##       With Split-Job:
##          Get-Content hosts.txt | Split-Job {%{ .\MyScript.ps1 -ComputerName $_ }} | Export-Csv results.csv
##
## Example with an imported function:
##       function Test-WebServer ($ComputerName) {
##           $WebRequest = [System.Net.WebRequest]::Create("http://$ComputerName")
##           $WebRequest.GetResponse()
##       }
##       Get-Content hosts.txt | Split-Job {%{Test-WebServer $_ }} -Function Test-WebServer | Export-Csv results.csv
##
## Version History
## 1.0    First version posted on poshcode.org
##        Additional runspace error checking and cleanup
## 0.93   Improve error handling: errors originating in the Scriptblock now
##        have more meaningful output
##        Show additional info in the progress bar (thanks Stephen Mills)
##        Add SnapIn parameter: imports (registered) PowerShell snapins
##        Add Function parameter: imports functions
##        Add SplitJobRunSpace variable; allows scripts to test if they are
##        running in a runspace
## 0.92   Add UseProfile switch: imports the PS profile
##        Add Variable parameter: imports variables
##        Add Alias parameter: imports aliases
##        Restart pipeline if it stops due to an error
##        Set the current path in each runspace to that of the calling process
## 0.91   Revert to v 0.8 input syntax for the script block
##        Add error handling for empty input queue
## 0.9    Add logic to distinguish between scriptblocks and cmdlets or scripts:
##        if a ScriptBlock is specified, a foreach {} wrapper is added
## 0.8    Adds a progress bar
## 0.7    Stop adding runspaces if the queue is already empty
## 0.6    First version. Inspired by Gaurhoth's New-TaskPool script
################################################################################

function Split-Job {
    param (
        $Scriptblock = $(throw 'You must specify a command or script block!'),
        [int]$MaxPipelines=10,
        [switch]$UseProfile,
        [string[]]$Variable,
        [string[]]$Function = @(),
        [string[]]$Alias = @(),
        [string[]]$SnapIn
    )

    function Init ($InputQueue){
        # Create the shared thread-safe queue and fill it with the input objects
        $Queue = [Collections.Queue]::Synchronized([Collections.Queue]@($InputQueue))
        $QueueLength = $Queue.Count
        # Do not create more runspaces than input objects
        if ($MaxPipelines -gt $QueueLength) {$MaxPipelines = $QueueLength}
        # Create the script to be run by each runspace
        $Script  = "Set-Location '$PWD'; "
        $Script += {
            $SplitJobQueue = $($Input)
            & {
                trap {continue}
                while ($SplitJobQueue.Count) {$SplitJobQueue.Dequeue()}
            } |
        }.ToString() + $Scriptblock

        # Create an array to keep track of the set of pipelines
        $Pipelines = New-Object System.Collections.ArrayList

        # Collect the functions and aliases to import
        $ImportItems = ($Function -replace '^','Function:') +
            ($Alias -replace '^','Alias:') |
            Get-Item | select PSPath, Definition
        $stopwatch = New-Object System.Diagnostics.Stopwatch
        $stopwatch.Start()
    }

    function Add-Pipeline {
        # This creates a new runspace and starts an asynchronous pipeline with our script.
        # It will automatically start processing objects from the shared queue.
        $Runspace = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspace($Host)
        $Runspace.Open()
        if (!$?) {throw "Could not open runspace!"}
        $Runspace.SessionStateProxy.SetVariable('SplitJobRunSpace', $True)

        function CreatePipeline {
            param ($Data, $Scriptblock)
            $Pipeline = $Runspace.CreatePipeline($Scriptblock)
            if ($Data) {
                $Null = $Pipeline.Input.Write($Data, $True)
                $Pipeline.Input.Close()
            }
            $Null = $Pipeline.Invoke()
            $Pipeline.Dispose()
        }

        # Optionally import profile, variables, functions and aliases from the main runspace
        if ($UseProfile) {
            CreatePipeline -Script "`$PROFILE = '$PROFILE'; . `$PROFILE"
        }
        if ($Variable) {
            foreach ($var in (Get-Variable $Variable -Scope 2)) {
                trap {continue}
                $Runspace.SessionStateProxy.SetVariable($var.Name, $var.Value)
            }
        }
        if ($ImportItems) {
            CreatePipeline $ImportItems {
                foreach ($item in $Input) {New-Item -Path $item.PSPath -Value $item.Definition}
            }
        }
        if ($SnapIn) {
            CreatePipeline (Get-PSSnapin $Snapin -Registered) {$Input | Add-PSSnapin}
        }
        $Pipeline = $Runspace.CreatePipeline($Script)
        $Null = $Pipeline.Input.Write($Queue)
        $Pipeline.Input.Close()
        $Pipeline.InvokeAsync()
        $Null = $Pipelines.Add($Pipeline)
    }

    function Remove-Pipeline ($Pipeline) {
        # Remove a pipeline and runspace when it is done
        $Runspace = $Pipeline.RunSpace
        $Pipeline.Dispose()
        $Runspace.Close()
        $Runspace.Dispose()
        $Pipelines.Remove($Pipeline)
    }

    # Main
    # Initialize the queue from the pipeline
    . Init $Input
    # Start the pipelines
    while ($Pipelines.Count -lt $MaxPipelines -and $Queue.Count) {Add-Pipeline}

    # Loop through the runspaces and pass their output to the main pipeline
    while ($Pipelines.Count) {
        # Show progress
        if (($stopwatch.ElapsedMilliseconds - $LastUpdate) -gt 1000) {
            $Completed = $QueueLength - $Queue.Count - $Pipelines.count
            $LastUpdate = $stopwatch.ElapsedMilliseconds
            $SecondsRemaining = $(if ($Completed) {
                (($Queue.Count + $Pipelines.Count)*$LastUpdate/1000/$Completed)
            } else {-1})
            Write-Progress 'Split-Job' ("Queues: $($Pipelines.Count)  Total: $($QueueLength)  " +
            "Completed: $Completed  Pending: $($Queue.Count)")  `
            -PercentComplete ([Math]::Max((100 - [Int]($Queue.Count + $Pipelines.Count)/$QueueLength*100),0)) `
            -CurrentOperation "Next item: $(trap {continue}; if ($Queue.Count) {$Queue.Peek()})" `
            -SecondsRemaining $SecondsRemaining
        }
        foreach ($Pipeline in @($Pipelines)) {
            if ( -not $Pipeline.Output.EndOfPipeline -or -not $Pipeline.Error.EndOfPipeline ) {
                $Pipeline.Output.NonBlockingRead()
                $Pipeline.Error.NonBlockingRead() | Out-Default
            } else {
                # Pipeline has stopped; if there was an error show info and restart it
                if ($Pipeline.PipelineStateInfo.State -eq 'Failed') {
                    $Pipeline.PipelineStateInfo.Reason.ErrorRecord |
                        Add-Member NoteProperty writeErrorStream $True -PassThru |
                            Out-Default
                    # Restart the runspace
                    if ($Queue.Count -lt $QueueLength) {Add-Pipeline}
                }
                Remove-Pipeline $Pipeline
            }
        }
        Start-Sleep -Milliseconds 100
    }
}