<#
.SYPNOPSIS
	Rebuild SQL Files from json conf files

.DESCRIPTION
	Rebuild SQL Files from json conf files

#>
[CmdletBinding()]
param(
	[parameter(
		ValueFromPipeline = $true
		, ValueFromPipelineByPropertyName = $true
		, Mandatory = $false
	)
	] 	
	[alias('f','FullName')][String[]]$Filename
)

#$ErrorActionPreference = ""
begin{
	$Error.Clear()
#	$local:ErrorActionPreference = "Continue"
	Write-Host "Start processing Rebuild file(s)" -BackgroundColor Green -ForegroundColor White
	Write-Host ""

	$ConfigCount = 1
	$ConfigCount = $ConfigCount
	$ErrorCount = 0

	function Start-ProcessConfigFiles
	{
		param(
			[parameter(ValueFromPipeline = $false, Mandatory = $false)]
		[String[]]$Filename
		)
		begin{
			$ErrorCount = 0
			$space = "  "
		}
		process{
			try{
				$Filename | ForEach-Object {
					try{
						$ConfigFile = $_
						Write-Host "::group::Config File: $($ConfigFile)"
						Write-Host "$($space)? Test file $($ConfigFile)..."

						if ( Test-Path -Path $ConfigFile ){
							Write-host "$($space)  ↳ File exists"

							Start-RebuildFromConfigFile $ConfigFile

							Write-Host "::endgroup::"
						} else {
							$message = "Config file not found $($ConfigFile)"
							Write-Host "::error file={$($ConfigFile)}::$($message)"
							throw [System.AggregateException]::new('$message', $ConfigFile)
						}
					}
					catch [System.AggregateException]{
						$ErrorCount++
						Write-Host "::endgroup::"
						$message = "Error(s) occured while processing Config file $($ConfigFile)" 
						Write-Host "::error file={$($ConfigFile)}::$($message)" -ForegroundColor Green
					}
					catch{
						Write-Host "unknown error" -ForegroundColor White -BackgroundColor Red
						throw $PSItem
					}
				}
			}
			catch{
				$ErrorCount++
				Write-Host "::endgroup::"
				Write-Host "::error::Start-ProcessConfigFiles unhandled exception"		-ForegroundColor Red	
				$message = $PSItem.Exception.Message
				$local:ErrorActionPreference = "Stop"
				$PSCmdlet.WriteError($PSItem)
			}
		}
		end{
			if ($ErrorCount -gt 0)
			{
				$message = "Error(s) occured while processing Config file(s) ($($ErrorCount))"
				Write-Host "::error file={$($Filename)}::$($message)" -ForegroundColor White -BackgroundColor Magenta
				throw [System.AggregateException]::new('$message', $ConfigFile)
			}
		}
	}
	function Start-RebuildFromConfigFile
	{
		param(
			[parameter(ValueFromPipeline = $false, Mandatory = $false)]
			[String]$Filename
		)
		begin{
			$ErrorCount = 0
			$space = "  "
		}
		process{
			try {
				Write-Host "$($space)~ Read JSON file: $($Filename)"
				try
				{
					$json = Get-Content -Path $Filename | ConvertFrom-Json					
				}
				catch
				{
					$message = $PSItem.Exception.Message
					Write-Host "$($space)::error file={$($Filename)}::$($message)"
					throw [System.AggregateException]::new('$message', $ConfigFile)
				}

				if ( $null -eq $json )
				{
					$message = "JSON file is null or empty"
					Write-Host "::error file={$($Filename)}::$($message)"
					throw [System.AggregateException]::new('$message', $ConfigFile)
				} 
				Write-Host "$($space)  ↳ Valid JSON file: $($Filename)"
				Write-Host "$($space)► Rebuild files: [$($json.count)]"
				
				$json | ForEach-Object{
					$build = $_
					$name = $build.name
					$file = $build.file
					$tempFile = $null
					Write-Host "$($space*2)▼ Build: $($name) [$($file)]"

					try{
						$file = [IO.Path]::GetFullPath($file, (Get-Location))
						Write-Host "$($space*3)↳ File: $($file)"
						$tempFile = [System.IO.Path]::GetTempFileName()
						Write-Host "$($space*3)↳ Temp File: $($tempFile)"

						$sources = $build.sources
						$id = 1
						$sources | ForEach-Object{
							$source = $_
							$path = $source.path.ToString()
							$exclude = $source.exclude
							$recurse = $source.recurse
				
							Write-Host "$($space*3)◢ path: $path"
				
							if ( Test-Path -Path $path.ToString() -Exclude $exclude ){			
								$childs = @{
									Path = $path.ToString()
									Exclude = $exclude
									Recurse = $recurse
								}
					
								Get-ChildItem @childs | Select-Object -ExpandProperty FullName | ForEach-Object { 
									$file_commit=$(git log -1 --date=iso-strict --pretty=format:"[ %h, %ad ]" -- $_)
									Write-Host "$($space*4)+ $_ $file_commit"	
									$regexVersion = '(.*--[\s|\t]+###[\s|\t]+\[[\s|\t]*Version[\s|\t]*\][\s|\t]*:).*$'
									$regexSource = '(.*--[\s|\t]+###[\s|\t]+\[[\s|\t]*Source[\s|\t]*\][\s|\t]*:).*$'
									$regexHash = '(.*--[\s|\t]+###[\s|\t]+\[[\s|\t]*Hash[\s|\t]*\][\s|\t]*:).*$'
									$regexEndOfFile = "(?s)\s*$"
									$replaceVersion=$(git log -1 --date=iso-strict --pretty=format:"%ad" -- $_)
									$replaceSource=$(git ls-files $_)
									$replaceHash="$(git log -1 --pretty=format:"%h" -- $_) [SHA256-$((Get-FileHash $_ -Algorithm SHA256).Hash)]"

									(Get-Content -Path $_ ) -replace $regexVersion, "`$1 $replaceVersion" -replace $regexSource, "`$1 $replaceSource" -replace $regexHash, "`$1 $replaceHash" | Out-String | ForEach-Object { $_ -replace $regexEndOfFile } | Add-Content -Path $tempFile
									"" | Add-Content -Path $tempFile
								}
							}
							else{
								$ErrorCount++
								Write-Host "::error file={$($path.ToString())}::File not found $($path.ToString())" -ForegroundColor Red
							}
							$id++
						}
						if (Test-Path -Path $tempFile) {
							Write-Host "$($space*3)> Save content to $file"

							if ( -not (Test-Path -Path $file))
							{
								"" | Set-Content -Path $file
							}

							if(Compare-Object -ReferenceObject $(Get-Content $tempFile -Raw) -DifferenceObject $(Get-Content $file -Raw)){
								Write-Host "$($space*3)> Replace file $file"
								Get-Content -Path $tempFile | Set-Content -Path $file
							} else {
								Write-Host "$($space*3)> Skip file $file (no differences)"
							}
							
							Remove-Item -Path $tempFile
						}
					}
					catch{
						$ErrorCount++
						Write-Host "::error file={$file}::File cannot be created: $($PSItem.Exception.Message)"
						#Write-Host $_.ScriptStackTrace
					}
				}				
			}
			catch [System.AggregateException]{
				$ErrorCount++
			}
			catch{
				$ErrorCount++
				Write-Host "::error::Start-RebuildFromConfigFile unhandled exception"		-ForegroundColor Red	
				$message = $PSItem.Exception.Message
				write-host $message
				$local:ErrorActionPreference = "Stop"
				$PSCmdlet.WriteError($PSItem)
			}
		}
		end{
			Write-Host "end rebuild from config" -ForegroundColor Black -BackgroundColor Gray
			if ($ErrorCount -gt 0)
			{
				throw [System.AggregateException]::new('$message', $ConfigFile)
			}
		}
	}
}

process{
	if ( $Filename.count -eq 0 ) {
		$PSCmdlet.ThrowTerminatingError(
		    [System.Management.Automation.ErrorRecord]::new(
        		([System.IO.InvalidDataException]"FileName parameter cannot be null or empty"), 'MissingValue', [System.Management.Automation.ErrorCategory]::ObjectNotFound, $Filename
		    )
		)		
	}

	try{
		Start-ProcessConfigFiles $Filename
	}
	catch [System.AggregateException]{
		$ErrorCount++
	}
	catch{
		$ErrorCount++
		$message = $PSItem.Exception.Message
		Write-Host "::error::$($message)"
	}
}
end{
	Write-Host "END ERROR = $ErrorCount"
	If ( $ErrorCount -gt 0)
	{
		Write-Host "END ERROR"
		$ErrorRecord = [System.Management.Automation.ErrorRecord]::new(
			([System.IO.InvalidDataException]"Execution stopped because one or more errors were encountered"), 'Error', [System.Management.Automation.ErrorCategory]::OperationStopped, $Filename
		)
		$PSCmdlet.ThrowTerminatingError($ErrorRecord)		
	}
}
