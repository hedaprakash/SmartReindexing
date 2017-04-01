#To run on remote server using sql authentication
$erroractionpreference = "Stop"
$ScriptDir = split-path -parent $MyInvocation.MyCommand.Path
$lServerName="testserver.contoso.com"
$SqlUser="validateSql"
$SqlPassword="testpass" 
Z:\Scripts\SmartReindexing\SmartReindexing.ps1 -lServerName $lServerName -SqlUser $SqlUser -SqlPassword $SqlPassword 

#To run on localhost and ignore read only db using windows authentication
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  

#To run on localhost and include read only db
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  -DoNotIgnoreReadOnlyDatabase


#To execute using sql authentication
$erroractionpreference = "Stop"
$SqlUser="validateSql"
$SqlPassword="testpass" 
D:\DBABIN\SmartReindexing.ps1  -SqlUser $SqlUser -SqlPassword $SqlPassword 

#To ignore certain tables from reindexing
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  -IgnoreTableForReindexing "PerformanceSecurity,AdvPortfolioTransaction"

#To execute "DBA_BackupDB.LogBackup" backup job
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  -LogBackupJobName "DBA_BackupDB.LogBackup"

#To execute with backup taken after 10 GB 
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  -BackupAfterChangePagesGB 10

#To execute with waiting upto 30 minute before ignore collecting fragmentation data (in seconds)
$erroractionpreference = "Stop"
D:\DBABIN\SmartReindexing.ps1  -FragCollTimeout 1800

