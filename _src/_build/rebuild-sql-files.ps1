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
#			$local:ErrorActionPreference = "Continue"
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
#							$local:ErrorActionPreference = "Continue"
							throw [System.AggregateException]::new('$message', $ConfigFile)
	#						Write-Host "::error file={$($ConfigFile)},line={},col={}::Config file not found $($ConfigFile)"
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
				#Write-Host "::error file={$($ConfigFile)}::$($message)"
				$local:ErrorActionPreference = "Stop"
				$PSCmdlet.WriteError($PSItem)
			}
		}
		end{
			if ($ErrorCount -gt 0)
			{
				$message = "Error(s) occured while processing Config file(s) ($($ErrorCount))"
				Write-Host "::error file={$($Filename)}::$($message)" -ForegroundColor White -BackgroundColor Magenta
				<#$ErrorRecord = [System.Management.Automation.ErrorRecord]::new(
					[System.IO.FileNotFoundException]"$($message)"
					, "OpenError"
					, [System.Management.Automation.ErrorCategory]::OpenError
					, $Filename
				)#>
#				$local:ErrorActionPreference = "Continue"
#				$local:ErrorActionPreference = "Stop"
#				$PSCmdlet.WriteError($ErrorRecord)
#				$local:ErrorActionPreference = "Stop"
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
#			$local:ErrorActionPreference = "Stop"
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

						#Write-Host "$($space*3)↳ Create file: $($file)"
						#Set-Content -Path $file -Value ""

						$sources = $build.sources
						$id = 1
						$sources | ForEach-Object{
							$source = $_
							$path = $source.path.ToString()
							$exclude = $source.exclude
							$recurse = $source.recurse
							#$main_date=$(git log -1 --date=iso-strict --pretty=format:"%ad")
							#$main_shorthash=$(git log -1 --pretty=format:"%h")=
							#$main_commitdate=$(git log -1 --date=iso-strict --pretty=format:"%cd")
				
							Write-Host "$($space*3)◢ path: $path"
				
							if ( Test-Path -Path $path.ToString() -Exclude $exclude ){			
								$childs = @{
									Path = $path.ToString()
									Exclude = $exclude
									Recurse = $recurse
								}
					
							#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"
							#Add-Content -Path $file -Value "-- ### [Date]: $($main_date)"
							#Add-Content -Path $file -Value "-- ### [Date]: $($main_commitdate)"
							#Add-Content -Path $file -Value "-- ### [Hash]: $($main_hash)"
							#Add-Content -Path $file -Value "-- ### [Hash]: $($main_shorthash)"
							#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"
				
								Get-ChildItem @childs | Select-Object -ExpandProperty FullName | ForEach-Object { 
									$file_commit=$(git log -1 --date=iso-strict --pretty=format:"[ %h, %ad ]" -- $_)
									Write-Host "$($space*4)+ $_ $file_commit"	
#									((Get-Content -Path $_ -Raw) -replace "(?s)\s*$"), "" | Add-Content -Path $tempFile		
									$regexVersion = '(.*--[\s|\t]+###[\s|\t]+\[[\s|\t]*Version[\s|\t]*\][\s|\t]*:).*$'
									$regexHash = '(.*--[\s|\t]+###[\s|\t]+\[[\s|\t]*Hash[\s|\t]*\][\s|\t]*:).*$'
									$regexEndOfFile = "(?s)\s*$"
									$replaceVersion=$(git log -1 --date=iso-strict --pretty=format:"%ad" -- $_)
									$replaceHash=$(git log -1 --pretty=format:"%h" -- $_)
									#Write-Host "$(git log -1 --pretty=full -- $_)"
									#write-host "xxxxx 11111111111"
#									Write-Host "$(git show -1 --pretty=full -- $_)"
									#write-host "xxxxx 22222222222"
									#Write-Host "$(git rev-list -1 -all -- $_)"
									#write-host "xxxxx 33333333333"
	
									(Get-Content -Path $_ ) -replace $regexVersion, "`$1 $replaceVersion" -replace $regexHash, "`$1 $replaceHash" | Out-String | ForEach-Object { $_ -replace $regexEndOfFile } | Add-Content -Path $tempFile
									"" | Add-Content -Path $tempFile
#									(Get-Content -Path $_ ) -replace $regexVersion, "`$1 $replaceVersion" -replace $regexHash, "`$1 $replaceHash" | Out-String | ForEach-Object { write-host "XXX $_" }								
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
#							(Get-Content -Path $tempFile -Raw) -replace "(?s)`r`n\s*$" | Set-Content -Path $file
							#$regexVersion = '(?<=-- ### [Version]:)[.]*$'
							#$regexHash = '(?<=-- ### [Hash]:)[.]*$'
							#(Get-Content -Path $tempFile ) -replace $regexVersion, "`$1 $replaceVersion" -replace $regexHash, "`$1 $replaceHash" | Out-String | % { $_ -replace $regexEndOfFile } | Set-Content -Path $file

							if ( -not (Test-Path -Path $file))
							{
								"" | Set-Content -Path $file
							}
							#Get-Content -Path $tempFile -Raw
							if(Compare-Object -ReferenceObject $(Get-Content $tempFile -Raw) -DifferenceObject $(Get-Content $file -Raw)){
								Write-Host "$($space*3)> Replace file $file"
								Get-Content -Path $tempFile | Set-Content -Path $file
							} else {
								Write-Host "$($space*3)> Keep file $file"
							}
							
							#( (Get-Content -Path $tempFile -Raw) -replace "(?s)`r`n\s*$" ).Split(@("`r`n", "`r", "`n"), [System.StringSplitOptions]::None)
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
				#Write-Host "::endgroup::"
				#$message = "Error(s) occured while reading Config file $($ConfigFile)" 
				#Write-Host "::error file={$($ConfigFile)}::$($message)" -ForegroundColor Green
			}
			catch{
				$ErrorCount++
				#Write-Host "::endgroup::"
				Write-Host "::error::Start-RebuildFromConfigFile unhandled exception"		-ForegroundColor Red	
				$message = $PSItem.Exception.Message
				write-host $message
				$local:ErrorActionPreference = "Stop"
				$PSCmdlet.WriteError($PSItem)
			}
<#			$json | ForEach-Object{
				$build = $_
				$name = $build.name
				$file = $build.file
				Write-Host "::group::Build: $($name) [$($file)]"
				#Write-Host "Build: $name"
				Write-Host " => file: $($file)"
		
				try{
				Set-Content -Path $file -Value ""
				}
				catch{
					Write-Host "An error occured:"
					Write-Host $_.ScriptStackTrace
				}
		
				$sources = $build.sources
				$id = 1
				$sources | ForEach-Object{
					$source = $_
					$path = $source.path
					$exclude = $source.exclude
					$recurse = $source.recurse
					#$main_date=$(git log -1 --date=iso-strict --pretty=format:"%ad")
					#$main_shorthash=$(git log -1 --pretty=format:"%h")
					#$main_commitdate=$(git log -1 --date=iso-strict --pretty=format:"%cd")
		
					Write-Host "    path: $path"
		
					$test = Test-Path -Path $path.ToString() -Exclude $exclude 
					Write-Host "test $test"
					if ( Test-Path -Path $path.ToString() -Exclude $exclude ){
		
						$childs = @{
							Path = $path.ToString()
							Exclude = $exclude
							Recurse = $recurse
						}
			
					#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"
					#Add-Content -Path $file -Value "-- ### [Date]: $($main_date)"
					#Add-Content -Path $file -Value "-- ### [Date]: $($main_commitdate)"
					#Add-Content -Path $file -Value "-- ### [Hash]: $($main_hash)"
					#Add-Content -Path $file -Value "-- ### [Hash]: $($main_shorthash)"
					#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"
		
						Get-ChildItem @childs | Select-Object -ExpandProperty FullName | ForEach-Object { 
							$file_commit=$(git log -1 --date=iso-strict --pretty=format:"[%h, %ad]" $_)
							Write-Host "     + $_ $file_commit"			
						}
					}
					else{
						Write-Host "An error occured: file not found"
					}
					$id++
			#		Get-Content -Path $path
			#		Write-Host "XXXXXXXXXXX $path"
			#		Get-ChildItem -Path $path
			#		Write-Host "XXXXXXXXXXX"
				}
				git add $file
				Write-Host "::endgroup::"
			}#>
		}
		end{
			Write-Host "end rebuild from config" -ForegroundColor Black -BackgroundColor Gray
			if ($ErrorCount -gt 0)
			{
				#$message = "Error(s) occured while reading Config file(s) ($($ErrorCount))"
				#Write-Host "  ::error file={$($Filename)}::$($message)" -ForegroundColor Yellow
				throw [System.AggregateException]::new('$message', $ConfigFile)
<#				Write-Host "::error file={$($Filename)}::$($message)" -ForegroundColor Red
				$ErrorRecord = [System.Management.Automation.ErrorRecord]::new(
					[System.IO.FileNotFoundException]"$($message)"
					, "OpenError"
					, [System.Management.Automation.ErrorCategory]::OpenError
					, $Filename
				)#>
#				$local:ErrorActionPreference = "Continue"
#				$local:ErrorActionPreference = "Stop"
#				$PSCmdlet.WriteError($ErrorRecord)
#				$local:ErrorActionPreference = "Stop"
			}
		}
	}
}

process{
	if ( $Filename.count -eq 0 ) {
		#Throw "FileName parameter cannot be null or empty"
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
#		throw
	}
}
end{
	Write-Host "END ERROR = $ErrorCount"
#	$ErrorCount = 1
	If ( $ErrorCount -gt 0)
	{
		Write-Host "END ERROR"
		$ErrorRecord = [System.Management.Automation.ErrorRecord]::new(
			([System.IO.InvalidDataException]"Execution stopped because one or more errors were encountered"), 'Error', [System.Management.Automation.ErrorCategory]::OperationStopped, $Filename
		)
		$PSCmdlet.ThrowTerminatingError($ErrorRecord)		
	}
}

	<#
	Write-Host "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
	#Write-Host $MyInvocation.MyCommand.Path
	#Write-Host $(Get-Item $MyInvocation.MyCommand.Path).Directory
	$config = Join-Path -Path $(Get-Item $MyInvocation.MyCommand.Path).Directory -ChildPath "rebuild.json"
	#Write-Host $test
	Write-Host "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

	$json = Get-Content -Path $config | ConvertFrom-Json

	#Write-Host "xxxxxxx"
	#Write-Host $json
	#Write-Host "xxxxxxx"
	#Write-Host ""
	$json | ForEach-Object{
		$build = $_
		$name = $build.name
		$file = $build.file
		Write-Host "::group::Build: $($name) [$($file)]"
		#Write-Host "Build: $name"
		Write-Host " => file: $($file)"

		try{
		Set-Content -Path $file -Value ""
		}
		catch{
			Write-Host "An error occured:"
			Write-Host $_.ScriptStackTrace
		}

		$sources = $build.sources
		$id = 1
		$sources | ForEach-Object{
			$source = $_
			$path = $source.path
			$exclude = $source.exclude
			$recurse = $source.recurse
			#$main_date=$(git log -1 --date=iso-strict --pretty=format:"%ad")
			#$main_shorthash=$(git log -1 --pretty=format:"%h")
			#$main_commitdate=$(git log -1 --date=iso-strict --pretty=format:"%cd")

			Write-Host "    path: $path"

			$test = Test-Path -Path $path.ToString() -Exclude $exclude 
			Write-Host "test $test"
			if ( Test-Path -Path $path.ToString() -Exclude $exclude ){

				$childs = @{
					Path = $path.ToString()
					Exclude = $exclude
					Recurse = $recurse
				}
	
			#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"
			#Add-Content -Path $file -Value "-- ### [Date]: $($main_date)"
			#Add-Content -Path $file -Value "-- ### [Date]: $($main_commitdate)"
			#Add-Content -Path $file -Value "-- ### [Hash]: $($main_hash)"
			#Add-Content -Path $file -Value "-- ### [Hash]: $($main_shorthash)"
			#Add-Content -Path $file -Value "----------------------------------------------------------------------------------------------------"

				Get-ChildItem @childs | Select-Object -ExpandProperty FullName | ForEach-Object { 
					$file_commit=$(git log -1 --date=iso-strict --pretty=format:"[%h, %ad]" $_)
					Write-Host "     + $_ $file_commit"			
				}
			}
			else{
				Write-Host "An error occured: file not found"
			}
			$id++
	#		Get-Content -Path $path
	#		Write-Host "XXXXXXXXXXX $path"
	#		Get-ChildItem -Path $path
	#		Write-Host "XXXXXXXXXXX"
		}
		git add $file
		Write-Host "::endgroup::"
	}

	git add $MyInvocation.MyCommand.Path
	#$env:GIT_COMMITTER_DATE=$main_date
	#git commit #--amend --no-edit
	#git commit --message="Rebuild after commit $($main_hash) \n $($main_shorthash) \n $($main_date)"
	#Remove-Item Env:\GIT_COMMITTER_DATE

	Write-Host "-- ### [Date]: $($main_date)"
	Write-Host "-- ### [Commit]: $($main_commitdate)"
	Write-Host "-- ### [Hash]: $($main_hash)"
	Write-Host "-- ### [Hash]: $($main_shorthash)"

	#Get-Location
	#Get-Content inputFile*.txt | Set-Content joinedFile.txt
	#Get-ChildItem -Path ./*.json -Recurse -Exclude project.json | Select-Object -ExpandProperty FullName
	Write-Host $MyInvocation.MyCommand
	Write-Host $MyInvocation.MyCommand.Path
#>
