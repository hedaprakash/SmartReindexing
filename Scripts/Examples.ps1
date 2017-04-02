#region login and password
#To run on remote server using sql authentication
$erroractionpreference = "Stop"
$ScriptDir = split-path -parent $MyInvocation.MyCommand.Path
$lServerName="W16S12e1"
$SqlUser="sa"
$SqlPassword="Sequoia2012"
#end region
 

\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1 -lServerName $lServerName -SqlUser $SqlUser -SqlPassword $SqlPassword 

#To run on localhost and ignore read only db using windows authentication
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  


#To run on localhost and include read only db
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -DoNotIgnoreReadOnlyDatabase

#To execute using sql authentication
$erroractionpreference = "Stop"
$SqlUser="sa"
$SqlPassword="Sequoia2012" 
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -SqlUser $SqlUser -SqlPassword $SqlPassword 


#To ignore certain tables from reindexing
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -IgnoreTableForReindexing "PerformanceSecurity,AdvPortfolioTransaction"

#To execute "DBA_BackupDB.LogBackup" backup job
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -LogBackupJobName "DBA:Backup All Tlogs"

#To execute with backup taken after 10 GB 
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -BackupAfterChangePagesGB 10

#To execute with waiting upto 30 minute before ignore collecting fragmentation data (in seconds)
$erroractionpreference = "Stop"
\\w12r2hv\SQLSetup\Scripts\SmartReindexing\SmartReindexing.ps1  -FragCollTimeout 1800

