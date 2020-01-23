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
$log = "$PSScriptRoot\logs\$timestamp.log"
$histdate = (Get-Date).ToString('yyyy/MM/dd')
#$histdate = '2020/01/22'
$fmtdate = $histdate -replace '/',''

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
	
	
	$SQLSTMT = "SELECT DISTINCT SYMBOL_ID, ID_NOTATION, DFILENO FROM mrkt.dbo.symbols WHERE exchange_id = 8 and (id_notation <> 0 or id_notation is not null) and symbol_id not in (105650324, 74098628, 74735303,74735592,77374275,31311530,33659699,33668371,33823308,32446988,76093832,92957777,31162962,33823307,98890743,87936514,95198845,95198847,100390265,135077790,95198846) ORDER BY SYMBOL_ID"
	
	$SQLCommand = New-Object System.Data.SqlClient.SqlCommand($SQLSTMT, $SQLConnection) 
    
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SQLCommand

    $DataSet = New-Object System.Data.DataSet
    $SqlAdapter.Fill($DataSet)

    $allTable = $DataSet.Tables[0].Rows

    foreach($row in $allTable)
    { 
		$idNotation = $row.id_notation
		$symb = $row.symbol_id	
		$dfile = $row.dfileno
		
		$link = "http://ofs-histcontrol.glb.fdsg.factset.com/HistoricalController.Core/api/Historical/EODSeries?customerId=CIBC&userId=-1&id_notation=$idNotation&realtime=false&begin=$histdate&end=$histdate"
		
		Write-Host $link

		$sqlcmd = New-Object System.Data.SqlClient.SqlCommand
		$sqlcmd.connection = $SQLConnection
		
		$sqlcmd.CommandText = "SET NOCOUNT ON; " +
			"if exists (select dte from data$dfile where dte=$fmtdate and symbol_id = $symb) " +
			"BEGIN " +
			"DELETE data$dfile where dte=$fmtdate and symbol_id = $symb " +
			"END " +
			"INSERT INTO dbo.data$dfile (symbol_id,dte,open_pr,high_pr,low_pr,close_pr,volume,adj_factor,data_src,shares_out,rolling_earnings,short_int,open_int,last_bid,last_ask,aux1,aux2,currency_id) " +
			"VALUES (@Symbol_id,@Date,@Open,@High,@Low,@Close,@Volume,@Adj_Factor,@Data_Src,@Shares_Out,@Rolling_Earnings,@Short_Int,@Open_Int,@Last_Bid,@Last_Ask,@Aux1,@Aux2,@Currency_Id); "
		
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Symbol_id",[Data.SQLDBType]::Int))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Date",[Data.SQLDBType]::Int))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Open",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@High",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Low",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Close",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Volume",[Data.SQLDBType]::DECIMAL, 12,0))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Adj_Factor",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Data_Src",[Data.SQLDBType]::TinyInt))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Shares_Out",[Data.SQLDBType]::DECIMAL, 12,0))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Rolling_Earnings",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Short_Int",[Data.SQLDBType]::DECIMAL, 12,0))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Open_Int",[Data.SQLDBType]::DECIMAL, 12,0))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Last_Bid",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Last_Ask",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Aux1",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Aux2",[Data.SQLDBType]::DECIMAL, 12,6))) | Out-Null
		$sqlcmd.Parameters.Add((New-Object Data.SqlClient.SqlParameter("@Currency_Id",[Data.SQLDBType]::Int))) | Out-Null

		Write-Host "Query prepared"
		Write-Host $sqlcmd.CommandText
		
		$res = Invoke-WebRequest $link -ErrorAction SilentlyContinue
		
		$data = $res.Content | ConvertFrom-Json
		if (!$?)
		{
			LogMsg "Can't  convert response from $link to JSON. Response: $data"        
			continue
		}

		foreach($item in $data)
		{   
					
			$date = Invoke-Expression "`$item.date"
			$first = Invoke-Expression "`$item.first"
			$last = Invoke-Expression "`$item.last"
			$low = Invoke-Expression "`$item.low"
			$high = Invoke-Expression "`$item.high"
			$tradingVolume = Invoke-Expression "`$item.tradingVolume"
					
			
			$date = $date.Substring(0,10)
			$date = $date -replace '-',''
			
			LogMsg "date:$date, first:$first, last:$last, low:$low, high:$high, volume:$tradingVolume"	
			
			$sqlcmd.Parameters[0].Value = $symb
			$sqlcmd.Parameters[1].Value = $date
			$sqlcmd.Parameters[2].Value = $first
			$sqlcmd.Parameters[3].Value = $high
			$sqlcmd.Parameters[4].Value = $low
			$sqlcmd.Parameters[5].Value = $last
			$sqlcmd.Parameters[6].Value = $tradingVolume
			$sqlcmd.Parameters[7].Value = 1.000000
			$sqlcmd.Parameters[8].Value = 1
			$sqlcmd.Parameters[9].Value = 0
			$sqlcmd.Parameters[10].Value = 0.000000
			$sqlcmd.Parameters[11].Value = 0
			$sqlcmd.Parameters[12].Value = 0
			$sqlcmd.Parameters[13].Value = 0.000000
			$sqlcmd.Parameters[14].Value = 0.000000
			$sqlcmd.Parameters[15].Value = 0.000000
			$sqlcmd.Parameters[16].Value = 0.000000
			$sqlcmd.Parameters[17].Value = 2

			$sqlcmd.ExecuteScalar() 
		}
	}	
}

#Error of connection 
catch 
{ 	
	$_.Exception.Response
	LogMsg "Error :  + $Error"  
	LogMsg "-------------------------End of $dbname scripts---------------------------------"
}

# Close the connection.
if ($SQLConnection.State -eq [Data.ConnectionState]::Open) 
{
	Write-Host "Connection is going to be closed"
	$SQLConnection.Close()
}


 