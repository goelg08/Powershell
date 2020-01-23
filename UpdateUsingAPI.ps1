#region Configuration
param (
    #Use script.cfg file from current script folder
    #config file can be redefined from command line
    [string]$env = "localhost",
    [string]$config_file ="$PSScriptRoot\IPO_main.cfg"
    
 )
 
 
#region Log functions
$startdate = (Get-Date).AddDays(-1).ToString('MM/dd/yyyy')
$enddate = (Get-Date).ToString('yyyy/dd/MM')
$timestamp = Get-Date -Format yyyyMMdd
$log = "$PSScriptRoot\log2\$timestamp.log"
$histdate = (Get-Date).ToString('yyyy/MM/dd')

function LogMsg
{
   param( [string]$message)
   #Write-Output ((Get-Date -Format s) + ":  $message")
   Write-Output ((Get-Date -Format s) + ":  [INFO] $message") | Out-File -FilePath $log -Append
}


try 
{ 
	$SQLConnection = New-Object System.Data.SqlClient.SqlConnection 		
	$SQLConnection.ConnectionString = "Server=FDSG01-PRD-SQL.prod.factset.com;Database=MRKT;User ID=ofssqlagentuser;Password=!ofssqlagentuser!;" 	
	$SQLConnection.Open() 
	

		$idNotation = 4359526
		$symb = 3377	

		$link = "http://ofs-histcontrol.glb.fdsg.factset.com/HistoricalController.Core/api/Historical/EODSeries?customerId=GENERAL_OFS&userId=-1&id_notation=$idNotation&realtime=false&begin=1998/01/01&end=2013/12/31"
		
		Write-Host $link
		
		$res = Invoke-WebRequest $link -UseBasicParsing
		if (!$?)
		{
			LogMsg "Web request to $link fail"        
			continue
		}
		
        Write-Host "$res"

		$data = $res.Content | ConvertFrom-Json
		if (!$?)
		{
			LogMsg "Can't  convert response from $link to JSON. Response: $data"        
			continue
		}

        Write-Host "date is : $data.date"

		foreach($item in $data)
		{   
					
			$date = Invoke-Expression "`$item.date"
			$first = Invoke-Expression "`$item.first"
			$last = Invoke-Expression "`$item.last"
			$low = Invoke-Expression "`$item.low"
			$high = Invoke-Expression "`$item.high"
			$tradingVolume = Invoke-Expression "`$item.tradingVolume"
			$tradingVolume = [Math]::Round($tradingVolume/100)
			
			$date = $date.Substring(0,10)
			$date = $date -replace '-',''

            		LogMsg "date:$date, first:$first, last:$last, low:$low, high:$high, volume:$tradingVolume"
			
			$cmdtxt = "update data30 set volume = $tradingVolume where symbol_id = $symb and dte = $date"
			Write-Host $cmdtxt
			
			$sqlcmd = New-Object System.Data.SqlClient.SqlCommand
			$sqlcmd.connection = $SQLConnection
			$sqlcmd.CommandTimeout = 600000
			$sqlcmd.CommandText = $cmdtxt



			 $sqlcmd.ExecuteNonQuery() 
		}	
}

#Error of connection 
catch 
{ 
	LogMsg "Error :  + $Error"  
	LogMsg "-------------------------End of $dbname scripts---------------------------------"
}

# Close the connection.
if ($SQLConnection.State -eq [Data.ConnectionState]::Open) 
{
	Write-Host "Connection is going to be closed"
	$SQLConnection.Close()
}


 