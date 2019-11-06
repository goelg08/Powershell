 <#
 
 This script performs the following processes :
 1. Truncate the tables.
 2. Fetch specific XML tags data and dump in some tables.
 3. Make a join between between above populated tables and store data in another table.
 
 #>


$timestamp = Get-Date -Format yyyyMMdd
$LogFile = "$PSScriptRoot\XML_Logs\$timestamp.log"

function LogMsg
{
   param( [string]$message)
   #Write-Output ((Get-Date -Format s) + ":  $message")
   Write-Output ((Get-Date -Format s) + ":  [INFO] $message") | Out-File -FilePath $LogFile -Append
}



##########################################
#Truncate tables to store lates data
##########################################
try {
		
	$cruise_build_tbl = "Test.dbo.Cruise_build"  
	$ccnet_config_tbl = "Test.dbo.ccnet_config"
	$config_details_tbl = "Test.dbo.config_details"

	$DataServer = "FDSG05-PRD-SQL.PROD.FACTSET.COM"
	$UserId = "ofssqlagentuser"
	$Password = "!ofssqlagentuser!"
	$Database = "Test"

	$SQLConnection = New-Object System.Data.SqlClient.SqlConnection
	$SQLConnection.ConnectionString = "Server=$DataServer;Database=Test;User ID=$UserId;Password=$Password ;"
	$SQLConnection.Open()



	$cmdtxt = "IF OBJECT_ID('$Cruise_build_tbl','U') IS NOT NULL TRUNCATE TABLE $Cruise_build_tbl IF OBJECT_ID('$ccnet_config_tbl','U') IS NOT NULL TRUNCATE TABLE $ccnet_config_tbl IF OBJECT_ID('$config_details_tbl','U') IS NOT NULL TRUNCATE TABLE $config_details_tbl"

	$sqlcmd = New-Object System.Data.SqlClient.SqlCommand
	$sqlcmd.connection = $SQLConnection
	$sqlcmd.CommandTimeout = 600000
	$sqlcmd.CommandText = $cmdtxt



	$sqlcmd.ExecuteNonQuery()
	$SQLConnection.Close()

	##################################################
	#Prepare cruise_build rows from XML
	##################################################

	$Cruise_build_DataTable = $null
	$Cruise_build_DataTable  = New-Object 'System.Data.DataTable';

	#Define the DataTable columns

	$Cruise_build_DataTable.Columns.Add("Target" ,"System.String" )
	$Cruise_build_DataTable.Columns.Add("BuildFile" ,"System.String" )
	$Cruise_build_DataTable.Columns.Add("Framework" ,"System.String" )

	$serviceStatePath = "D:\CIBC_Historical\XML_File\cruise.stable.build"
	[xml]$xml = Get-Content $serviceStatePath


	$nodes = $xml.SelectNodes("//project/target")

	foreach ($node in $nodes){
	   $nRow = $Cruise_build_DataTable.NewRow()
	   
	   $nRow."Target"        = $node.name
	   $nRow."BuildFile"   = $node.nant.buildfile
	   $nRow."Framework"   = $node.nant.properties.property[2].value  

	   $Cruise_build_DataTable.Rows.Add($nRow)

	 }

	##################################################
	#Dump Rows prepared above in cruise_build table
	##################################################

 	$sqlConnection = new-object System.Data.SqlClient.SqlConnection("Server=$DataServer; Database=$Database; User ID = $UserId; Password = $Password;")
	$sqlConnection.open()
	$SqlBulkCopy = new-object ("System.Data.SqlClient.SqlBulkCopy") $sqlConnection

	$SqlBulkCopy.DestinationTableName = "dbo.Cruise_build"
	$SqlBulkCopy.WriteToServer($Cruise_build_DataTable)

	$sqlConnection.Close()  


	##################################################
	#Prepare ccnet_config rows from XML
	##################################################
	
	$ccnet_config_DataTable = $null
	$ccnet_config_DataTable  = New-Object 'System.Data.DataTable';

	#Define the DataTable columns

	$ccnet_config_DataTable.Columns.Add("ProjectName" ,"System.String" )
	$ccnet_config_DataTable.Columns.Add("SVNLocation" ,"System.String" )

	$serviceStatePath = "D:\CIBC_Historical\XML_File\ccnet.config"
	[xml]$xml = Get-Content $serviceStatePath

	$nodes = $xml.SelectNodes("//cruisecontrol/project")

	foreach ($node in $nodes){
	   $nRow = $ccnet_config_DataTable.NewRow()

	   $nRow."ProjectName"        = $node.name
	   $nRow."SVNLocation"   = $node.sourcecontrol.sourceControls.svn.trunkUrl

	   $ccnet_config_DataTable.Rows.Add($nRow)

	 }

	##################################################
	#Dump Rows prepared above in ccnet_config table
	##################################################

	$sqlConnection = new-object System.Data.SqlClient.SqlConnection("Server=$DataServer; Database=$Database; User ID = $UserId; Password = $Password;")
	$sqlConnection.open()
	$SqlBulkCopy = new-object ("System.Data.SqlClient.SqlBulkCopy") $sqlConnection

	$SqlBulkCopy.DestinationTableName = "dbo.ccnet_config"
	$SqlBulkCopy.WriteToServer($ccnet_config_DataTable)

	$sqlConnection.Close()  #Close SQL connection
 
	########################################################  
	#Exec Proc to store Data in Config_Details Table
	########################################################  

	$DataServer = "FDSG05-PRD-SQL.PROD.FACTSET.COM"
	$UserId = "ofssqlagentuser"
	$Password = "!ofssqlagentuser!"
	$Database = "Test"

	$SQLConnection = New-Object System.Data.SqlClient.SqlConnection
	$SQLConnection.ConnectionString = "Server=$DataServer;Database=Test;User ID=$UserId;Password=$Password ;"
	$SQLConnection.Open()

	$cmdtxt = "EXEC p_GetConfigDetails"

	$sqlcmd = New-Object System.Data.SqlClient.SqlCommand
	$sqlcmd.connection = $SQLConnection
	$sqlcmd.CommandTimeout = 600000
	$sqlcmd.CommandText = $cmdtxt

	$sqlcmd.ExecuteNonQuery()
	$SQLConnection.Close()
}

Catch
{  
   
    $ErrorMessage = $_.Exception.Message | Out-File $logFile -Append;
    $FailedItem = $_.Exception.ItemName  | Out-File $logFile -Append;

    IF ($OleDbConnection.State -eq "Open"){ $OleDbConnection.Close();}
    IF ($sqlConnection.State -eq "Open")  { $sqlConnection.Close();  }
 
    Throw; 
}
Finally
{
    LogMsg -LogFile $LogFile -Message "Completed Executing the Powershell script";
}