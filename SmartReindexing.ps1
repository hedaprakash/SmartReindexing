param (
    [string]$lServerName = "",
    [string]$SqlUser = "",
    [string]$SqlPassword = "",
    [switch]$DoNotIgnoreReadOnlyDatabase,
    [Int]$FragCollTimeout= 0,
    [Int]$BackupAfterChangePagesGB= 0,
    [string]$IgnoreTableForReindexing = "",
    [string]$LogBackupJobName = ""
    )

$parameterpassed= "Parameters Passed:"
$parameterpassed+= "`nlServerName: $lServerName"
$parameterpassed+= "`nSqlUser: $SqlUser"
$parameterpassed+= if ($SqlPassword.Length -ne 0){"`nSqlPassword passed"} else {"`nSqlPassword not passed, windows authentication will be used"}
$parameterpassed+= if ($DoNotIgnoreReadOnlyDatabase.IsPresent){"`nDoNotIgnoreReadOnlyDatabaseflg switch was enabled"} else {"`nDoNotIgnoreReadOnlyDatabaseflg  switch was not enabled"}
$parameterpassed+= "`nBackupAfterChangePagesGB: $BackupAfterChangePagesGB"
$parameterpassed+= "`nFragCollTimeout: $FragCollTimeout"
$parameterpassed+= "`nLogBackupJobName: $LogBackupJobName"
$parameterpassed+= "`nIgnoreTableForReindexing: $IgnoreTableForReindexing"

#+-------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |    
#|{>/-------------------------------------------------------------\<}| 
#|: | Script Name: SmartReindexing.ps1                               | 
#|: | Author:  Prakash Heda                                          | 
#|: | Email:   Pheda@advent.com	 Blog:www.sqlfeatures.com   		 |
#|: | Purpose: Automated Reindex Jobs	                             |
#|: | 							 	 								 |
#|: |Date       Version ModifiedBy    Change 						 |
#|: |05-16-2012 1.0     Prakash Heda  Initial version                |
#|: |07-25-2013 1.1     Prakash Heda  Working for SQL 2012           |
#|: |05-02-2014 1.2     Hua Yu  Optional Support for Read Only       |
#|: |                   databases                                    |
#|: |07-25-2014 1.3     Prakash Heda  Working for SQL 2014           |
#|: |09-27-2014 1.4     Prakash Heda  Added Comments for             |
#|: |04-27-2016 1.5     Prakash Heda  Updated debugging              |
#|: |05-24-2016 2.0     Prakash Heda  Releasing version 2            |
#|: |05-24-2016 2.1     Prakash Heda  Bug Fixes and added logging    |
#|: |08-22-2016 2.2     Prakash Heda  Bug Fixes                      |
#|{>\-------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |  
#+-------------------------------------------------------------------+  


#region CommonCode
# Execute this to ensure Powershell has execution rights
#Set-ExecutionPolicy Unrestricted -Force
#Set-ExecutionPolicy bypass -Force
#Import-Module ServerManager 
#Add-WindowsFeature PowerShell-ISE
#+-------------Common Code started-----------------------------------+    
CLS

$ScriptLocation=split-path -parent $MyInvocation.MyCommand.Path
$ScriptNameWithoutExt=[system.io.path]::GetFilenameWithoutExtension($MyInvocation.MyCommand.Path)
$runtime=Get-Date -format "yyyy-MM-dd HH:mm:ss"
if ($lServerName.length -eq 0) {$lServerName  = gc env:computername}
$result= Test-Path C:\WINDOWS\Cluster\CLUSDB
switch ($result)
    {TRUE{$split = $lServerName.split("-");$lServerName = $split[0]}}

if ($BackupAfterChangePagesGB.length -ne 0) {$BackupAfterNoOfChangePages=($BackupAfterChangePagesGB*1024*1024)/8} else {$BackupAfterNoOfChangePages=300000}

if ($IgnoreTableForReindexing.length -ne 0) {$IgnoreTableForReindexingFormatted=$IgnoreTableForReindexing.Replace(",","','")} else {$IgnoreTableForReindexingFormatted="''"}

if ($FragCollTimeout -eq 0) {$FragCollTimeout = 1200}
#$lServerName = "vSacAxDb28-1"

$AppErrorCollection=@()

# HTML table Formatting for email
$a = "<style>"
$a = $a + "BODY{background-color:peachpuff;}"
$a = $a + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
$a = $a + "TH{border-width: 1px;padding: 0px;border-style: solid;border-color: black;background-color:thistle}"
$a = $a + "TD{border-width: 1px;padding: 0px;border-style: solid;border-color: black;background-color:palegoldenrod}"
$a = $a + "tr.datacellcolor {background-color: #CC9999; color: black;}"
$a = $a + "td.datacellgreen {background-color: #CC9999; color: black;}"
$a = $a + "td.datacellred {background-color: #CC9999; color: black;}"
$a = $a + "td.datacellYellow {background-color: #CC9999; color: black;}"
$a = $a + "td.datacellthistle {background-color: #CC9999; color: black;}"
$a = $a + "</style>"


# LoadSQLModule
if ( Get-PSSnapin -Registered | where {$_.name -eq 'SqlServerProviderSnapin100'} ) 
{ 
    if( !(Get-PSSnapin | where {$_.name -eq 'SqlServerProviderSnapin100'})) 
    {  
        Add-PSSnapin SqlServerProviderSnapin100 | Out-Null 
    } ;  
    if( !(Get-PSSnapin | where {$_.name -eq 'SqlServerCmdletSnapin100'})) 
    {  
        Add-PSSnapin SqlServerCmdletSnapin100 | Out-Null 
    } 
} 
else 
{ 
    if (Get-Module -ListAvailable -name "sqlps")
    {  
	    if (!(Get-Module  -name "sqlps")) 
	    {  
	        Import-Module 'sqlps' –DisableNameChecking  | Out-Null 
	    } 
	}
	else
	{
		write-host "${runtime}: SQL Powershell Module is not installed on this server"
	}
} 


function ExecuteQueryV2 (
  [string] $ServerInstance = $(throw "DB Server Name must be specified."),
  [string] $Database = "master",
  [string] $Query = $(throw "QueryToExecute must be specified."),
  [string] $ReadIntentTrue = $null,
  [int] $QueryTimeout=60000
  )
{
    Try
    {

        $TestSqlAcces=$false
        if ($SqlPassword.Length -ne 0)
        {	    
            $ReturnResultset=invoke-sqlcmd -ServerInstance $ServerInstance -database $Database -Query $Query -QueryTimeout $QueryTimeout -Username $SqlUser -Password $SqlPassword   -Verbose  -ErrorAction Stop 
        }
        else
        {
            $ReturnResultset=invoke-sqlcmd -ServerInstance $ServerInstance -database $Database -Query $Query -QueryTimeout $QueryTimeout -Verbose  -ErrorAction Stop 
        }
        $TestSqlAcces=$true
        $sqlresult=$ReturnResultset
        if ($sqlresult -match "Timeout expired")
        {$SQLTimeoutExpired=$True}

    }
        Catch 
        {
            Write-Warning $_.exception.message
            $errorMsg=$_.exception.message

            if ($errorMsg -match "A network-related or instance-specific error occurred while establishing a connection to SQL Server")
            {$SQLPortissue=$True}

            if ($errorMsg -match "Timeout expired")
            {$SQLTimeoutExpired=$True}

            if ($errorMsg -match "Access is denied.")
            {$Authenticationfailed=$True}
            
            $TestSqlAcces=$false
        }

        $QueryTable="testtblStoreQuery";$QueryExecuted = New-Object system.Data.DataTable “$QueryTable”
        $col1 = New-Object system.Data.DataColumn QueryToExecute,([string])
        $QueryExecuted.columns.add($col1)
        $row2 = $QueryExecuted.NewRow();$row2.QueryToExecute = $QueryToExecute ;$QueryExecuted.Rows.Add($row2)

        $functionOutput=[pscustomobject]   @{
        QueryExecuted=$QueryExecuted
        TestSqlAcces = $TestSqlAcces; sqlresult = $sqlresult
        DestinationHost=$ServerInstance
        DatabaseName=$DatabaseName
        ReturnServerName=$ReturnServerName
        UserName = $UserName; ExecuteSQLError = $errorMsg
        SQLPortIssue=$SQLPortissue
        Authenticationfailed=$Authenticationfailed
        SQLTimeoutExpired=$SQLTimeoutExpired
        }
        return $functionOutput
}




function write-PHLog {
    param([string] $Logtype,
          [string] $fileName,
          [switch] $echo,
          [switch] $clear
    )
    switch ($Logtype) 
        { 
            "DEBUG" {$LogtypeEntry="White";$cmdPrint="White"} 
            "DEBUG2" {$LogtypeEntry="White";$cmdPrint="White"} 
            "WARNING" {$LogtypeEntry="Magenta";$cmdPrint="Magenta"} 
            "ERROR" {$LogtypeEntry="Red";$cmdPrint="Red"} 
            "Success" {$LogtypeEntry="LightGreen";$cmdPrint="Green"} 
            default {$LogtypeEntry="DarkRed";$cmdPrint="RED"}
        }


$CurrentPath=$pwd
set-location c:\ -PassThru | out-null

    Try {            
            $LogToWriteRec=@()
            $flgObject=$false
            $input | %{
                    $LogToWriteRec+=$_; 
                    if ($_.length -gt 0) {if ($_.GetType().name -ne "String") {$flgObject=$true}} else {$flgObject=$true}
                }
            $LogToWrite =$LogToWriteRec | Out-String
            if (($Logtype -eq "Debug2")-and ($flgObject -ne $true) -and ($LogToWrite.Length -lt 120))
            {
                $LogToWrite="          "+$LogToWrite
            }
            $LogToWrite=$LogToWrite.TrimEnd()

            if ($functionname) {$LogToWrite= "        "+$functionname +":"+ $LogToWrite}
            if ($echo.IsPresent)
            {
                Write-host $LogToWrite -ForegroundColor $cmdPrint
            }

            if (($flgObject -eq $true) -or ($LogToWrite.Length -gt 129)) 
            {
                $LogToWrite="<blockquote>" + $LogToWrite + "</blockquote>"
            }

            $LogToWrite=($LogToWrite).Replace("`n","<br>")
            $LogToWrite=($LogToWrite).Replace("  ","&nbsp;&nbsp;")
            [boolean] $isAppend = !$clear.IsPresent
            if (($isAppend -eq $false ) -or (!(test-path $ExecutionSummaryLogFile)) )
            {$BodyColor="<body bgcolor=""DarkBlue"">"} else {$BodyColor=""}

            $BodyColor + "<font color=""$LogtypeEntry"">" + $LogToWrite  + "</font> <br>"  | out-file $ExecutionSummaryLogFile -encoding UTF8 -Append:$isAppend | Out-Null
            Start-Sleep -Milliseconds 10
            #<blockquote>Whatever you need to indent</blockquote>
        }
        catch
        {
            write-warning ("Write-PHLog function failed, very unsual pleae check with script writer`n`n $($_.exception.message) `n`n"  )
        }
    set-location $CurrentPath | out-null
}





#+-------------Common Code eded-----------------------------------+    
#endregion

#region ConfigureVariables

set-location "C:\" -PassThru | Out-Null 
set-location $ScriptLocation -PassThru | Out-Null 


$Logtime=Get-Date -format "yyyyMdHHmmss"
$LogPath=$ScriptLocation + "\pslogs\"
if(!(test-path $LogPath)){[IO.Directory]::CreateDirectory($LogPath)}
$ExecutionSummaryLogFile=$LogPath + $lServerName.Replace("-","_").Replace("\","_")  + "_" + $ScriptNameWithoutExt +  "_ExecutionSummary_" + $Logtime + ".html"
"Starting reindexing process"| write-PHLog -Logtype Success
$ReindexStatusErrorSummary = $LogPath + "AllReindexingErrors_" + $Logtime + ".html"

$LogName= $LogPath + $lServerName.Replace("-","_") + "_" +$Logtime + ".log"
$SQLGenerateDBReindexOutput= $LogPath + $lServerName.Replace("-","_") +  "_" +"GenerateDBReindex" + "_" +$Logtime + ".txt"
$SQLcmdbatfile= $ScriptLocation+ "\pslogs\" +"executeReindex" + "_" +$Logtime + ".bat"

$startSumamry="`r`nLogName: $ExecutionSummaryLogFile`r`nScriptLocation: $ScriptLocation`r`n"

$startSumamry | write-PHLog -echo -Logtype Debug2

$parameterpassed | write-PHLog -echo -Logtype Debug2

"Check msdb..tbl_indexRebuild_Log for more details"| write-PHLog -echo -Logtype Debug

#endregion

$ErrorActionPreference = "STOP"


#region PrepareReinDexStats
$reIndexruntime_Collect= "Start collecting index stats $($runtime)" 
$reIndexruntime_Collect | write-PHLog -Logtype Debug2

if ($DoNotIgnoreReadOnlyDatabase.IsPresent){$DoNotIgnoreReadOnlyDatabaseflg=1} else {$DoNotIgnoreReadOnlyDatabaseflg=0}

#$DoNotIgnoreReadOnlyDatabaseflg=1

$PrepareIndexFragmentation= @"


IF (OBJECT_ID('msdb..tbl_indexRebuild_Log','U') is null)
BEGIN
	CREATE TABLE msdb..tbl_indexRebuild_Log (
	logID INT IDENTITY(1,1) primary key clustered,
	insertDate DATETIME default getdate(),
	indexRebuildCommand NVARCHAR(2000),
	returnValue NVARCHAR(max)
	)
END

delete from msdb..tbl_indexRebuild_Log where insertDate < dateadd(Month,-1,getdate())
GO
INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Reindexing Started','Success')
GO

-- Create base table to store fragmented data 
if object_id('tempdb..fragmentedTables') is not null drop table tempdb..fragmentedTables
GO
CREATE TABLE tempdb..[fragmentedTables](
	Rowidnum bigint,
	[DatabaseName] [varchar](200) NULL,
	[TableName] [varchar](200) NULL,
	[INDEX_ID] [int] NULL,
	[INDEX_TYPE_DESC] [varchar](200) NULL,
	[AVG_FRAGMENTATION_IN_PERCENT] [float] NULL,
	page_count int null,
	[INDEX_Name] [varchar](500) NULL,
	CURRENT_FILL_FACTOR int NULL,
	EXPECTED_FILL_FACTOR int NULL,
	[SchemaName] varchar(200),
    [FullObjectName] varchar(200),
	[object_id] int NULL,
	[database_id] int NULL,
	[flg_Online] bit NULL,
	[TimeoutValue] Varchar(200) NULL,
	[SubtotalPages] bigint NULL,
	[flgBackupLog] bit NULL,
	Updateability varchar(200),
	[flgUpdateability] int NULL
) ON [PRIMARY]
GO

-- Collect table fragmentation 
if object_id('tempdb..CollectFragmentationDetails908') is not null drop table tempdb..CollectFragmentationDetails908
go
select *
into tempdb..CollectFragmentationDetails908
FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (db_id('master'),NULL,NULL,NULL,NULL ) a
where 1=2

if (1=$DoNotIgnoreReadOnlyDatabaseflg)
select name from sys.databases where name not in ( 'master','Model') 
else
select name from sys.databases where name not in ( 'master','Model') and is_read_only = 0


"@

$PrepareIndexFragmentation|clip

try
{
    $ActivityName="Preparing index fragmentation collection table: msdb..tbl_indexRebuild_Log on server: $lServerName "
    $ActivityName | write-PHLog  -echo -Logtype debug2
    $retPrepareIndexFragmentation=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $PrepareIndexFragmentation -QueryTimeout 60 -Verbose  
    $retPrepareIndexFragmentationResult= $retPrepareIndexFragmentation.sqlresult
    $retPrepareIndexFragmentationResult |  write-PHLog  -echo -Logtype debug2
    if (($retPrepareIndexFragmentation.SQLTimeoutExpired -eq $true) -or ($retPrepareIndexFragmentation.ExecuteSQLError -ne $null))
    {
        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($PrepareIndexFragmentation.Replace("'","''"))','$($retPrepareIndexFragmentation.ExecuteSQLError)')"
        $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($retPrepareIndexFragmentation.ExecuteSQLError) }
        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
        "Preparing index fragmentation collection table: msdb..tbl_indexRebuild_Log on server: $lServerName Error: `n $($retPrepareIndexFragmentation.ExecuteSQLError) "| write-PHLog  -echo -Logtype Warning
        "PrepareIndexFragmentation step failed : pleaes troubleshoot "| write-PHLog  -echo -Logtype Error
    }
    $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($PrepareIndexFragmentation.Replace("'","''"))','Started')"
    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
}
catch
{

    "Error while preparing index fragmentation collection table: $lServerName "| write-PHLog  -echo -Logtype Error
    $errMessage=$_.exception.message
    $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
    $errMessage | write-PHLog  -echo -Logtype Error
    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($PrepareIndexFragmentation.Replace("'","''"))','$errMessage')"
    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
}


if ($retPrepareIndexFragmentation.TestSqlAcces -eq $true) 
{


foreach ($dbname in $retPrepareIndexFragmentationResult)
{
    $dbname=$dbname.Name
    $ActivityName="Collecting fragmentation information from $dbname "
    $ActivityName | write-PHLog  -echo -Logtype debug2
    $CollectDBIndexFragmentation= @"
        Insert into  tempdb..CollectFragmentationDetails908  select * FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (db_id('$dbname'),NULL,NULL,NULL,NULL ) a 
"@
    $CollectDBIndexFragmentation

    Try 
    {
        $retDBIndexFragmentation=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $CollectDBIndexFragmentation -QueryTimeout $FragCollTimeout  -Verbose  
        $retDBIndexFragmentationResult= $retDBIndexFragmentation.sqlresult
        $retDBIndexFragmentation
        if (($retDBIndexFragmentation.SQLTimeoutExpired -eq $true) -or ($retDBIndexFragmentation.ExecuteSQLError -ne $null))
        {
            
            $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($CollectDBIndexFragmentation.Replace("'","''"))','$($retDBIndexFragmentation.ExecuteSQLError)')"
            $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($retDBIndexFragmentation.ExecuteSQLError) }
            ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
            "Collecting fragmentation information from $dbname : $($retDBIndexFragmentation.ExecuteSQLError) "| write-PHLog  -echo -Logtype Warning
            "Reindexing is not able to run on DB: $dbname, timeout value was $FragCollTimeout seconds, pleaes reindex manually for this db "| write-PHLog  -echo -Logtype Error
        }
        else
        {
            $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($CollectDBIndexFragmentation.Replace("'","''"))','Success')"
            ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
        }

    }
    catch
    {

        "Error while collecting index fragmentation collection table: $lServerName..$dbname "| write-PHLog  -echo -Logtype Error
        $errMessage=$_.exception.message 
        $errMessage | write-PHLog  -echo -Logtype Error
        $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($CollectDBIndexFragmentation.Replace("'","''"))','$errMessage')"
        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
    }
}

#endregion

#region GenerateDBReindexCode
# Code to generate reindex syntax

$GenerateDBReindexCode= @"
set nocount on
GO

-- Collect online vs offline tables
truncate table tempdb..fragmentedTables

GO

if object_id('tempdb..##tmpFragmentationDetails908_2') is not null drop table ##tmpFragmentationDetails908_2
go
select *,1 as flg_Online, OBJECT_SCHEMA_NAME([object_id], [database_id]) as schemaName
, convert(varchar(500),'') as IndexNAME
,0 as fill_factor,60+(CEILING (page_count/1000)) as TimeOutvalue
,OBJECT_NAME(OBJECT_ID,database_id) as TableName
into ##tmpFragmentationDetails908_2
from tempdb..CollectFragmentationDetails908
where AVG_FRAGMENTATION_IN_PERCENT > 20 or INDEX_TYPE_DESC = 'HEAP'

-- 

delete from ##tmpFragmentationDetails908_2 where AVG_FRAGMENTATION_IN_PERCENT < 15 or INDEX_TYPE_DESC = 'HEAP' or tablename like '#%' -- or page_count<20

Delete from ##tmpFragmentationDetails908_2 where tablename in ('$($IgnoreTableForReindexingFormatted)')

-- select TimeOutvalue,* from ##tmpFragmentationDetails908_2 where INDEX_TYPE_DESC <> 'HEAP' and AVG_FRAGMENTATION_IN_PERCENT>90 and page_count>40


-- find indexes to be done offline
-------------------------------------------------------------
declare @offlineIndexes table
(
	objectid bigint,
	databaseid bigint
)

DECLARE @command VARCHAR(5000)  

SELECT @command = 'Use [' + '?' + '] SELECT  distinct
object_id,db_id(''?'') from [?].sys.columns c 
where (c.system_type_id IN (34,35,99,241) or c.max_length =-1)
'  

--select @command
INSERT INTO @offlineIndexes  
EXEC sp_MSForEachDB @command  

/*

SELECT  distinct
object_id,db_id('testdb') from testdb.sys.columns c 
where (c.system_type_id IN (34,35,99,241) or c.max_length =-1)

*/



--select * from @offlineIndexes a
--join ##tmpFragmentationDetails908_2 b  
--on a.databaseid=b.database_id and a.objectid=b.object_id

update a set
flg_Online= 0
from ##tmpFragmentationDetails908_2 a	
join @offlineIndexes o on o.objectid =a.object_id and o.databaseid=a.database_id

-- select object_name (object_id,database_id),* from ##tmpFragmentationDetails908_2 where database_id=5


-- Update default fill factor to 90
if object_id('tempdb..##tmpFragmentationDetails908_2_INDEX') is not null drop table ##tmpFragmentationDetails908_2_INDEX
create table ##tmpFragmentationDetails908_2_INDEX 
(
	object_id BIGINT,
	INDEX_ID bigint,
	database_id bigint,
	IndexNAME varchar(2000),
	fILL_fACTOR bigint
)
SELECT @command = 'Use [' + '?' + '] 
select 
	a.object_id,a.INDEX_ID, a.database_id,b.name,b.FILL_FACTOR 
FROM   ##tmpFragmentationDetails908_2 a
join [?].sys.indexes b
on a.object_id = b.object_id
and a.INDEX_ID=b.INDEX_ID
and a.database_id=db_id(''?'')
'  
--select @command
INSERT INTO ##tmpFragmentationDetails908_2_INDEX  
EXEC sp_MSForEachDB @command  

--SELECT * FROM ##tmpFragmentationDetails908_2_INDEX where database_id=5


update a set 
	IndexNAME = b.IndexNAME,FILL_FACTOR =b.FILL_FACTOR 
-- select b.objectname ,*
FROM   ##tmpFragmentationDetails908_2 a
join ##tmpFragmentationDetails908_2_INDEX b
on a.object_id = b.object_id
and a.INDEX_ID=b.INDEX_ID
and a.database_id=b.database_id

-- SELECT * FROM ##tmpFragmentationDetails908_2_INDEX where database_id=5

-- truncate table TEMPDB..fragmentedTables 
insert into TEMPDB..fragmentedTables 
select 
	ROW_NUMBER() OVER(ORDER BY database_id,a.OBJECT_ID, INDEX_TYPE_DESC desc) AS Rowidnum,
	db_name(database_id),TableName
	,index_id,index_type_desc,avg_fragmentation_in_percent,page_count
	,IndexNAME,FILL_FACTOR,isnull(nullif(FILL_FACTOR,0),90)
	,schemaName,'['+ db_name(Database_ID) + ']' +'.' +'['+  a.SchemaName + ']' + '.' +'['+  TableName + ']' 
	,object_id,database_id,flg_Online,TimeOutvalue,0,0,convert(varchar(200),DATABASEPROPERTYEX ( db_name(database_id) , 'Updateability' ) ), 0 
from ##tmpFragmentationDetails908_2 a


-- SELECT * FROM TEMPDB..fragmentedTables where databaseName='tempdb'
-- SELECT * FROM TEMPDB..##tmpFragmentationDetails908_2 where db_name(Database_ID)='tempdb'

-- get total number of pages for backup logs
update a set
-- select 
SubTotalpages= (SELECT SUM(b.page_count)
                       FROM tempdb..fragmentedTables b
                       WHERE b.Rowidnum <= a.Rowidnum) 
-- select * 
from tempdb..fragmentedTables a

declare @nlitevalue bigint,@LogBackupPageCount bigint
update a set flgBackupLog=1 
-- select *
from tempdb..fragmentedTables a
where rowidnum in (
select  max(rowidnum)  as LogBackupID 
from tempdb..fragmentedTables a
group by SubtotalPages/$BackupAfterNoOfChangePages)



-- updating read only database
update a set flgUpdateability =1
-- select *
from tempdb..fragmentedTables a 
join (select DatabaseName, Min (Rowidnum)  as MinRowidnum from tempdb..fragmentedTables a where Updateability='Read_Only' group by DatabaseName) b
on a.DatabaseName= b.DatabaseName
and a.Rowidnum = b.MinRowidnum



update a set flgUpdateability =2
-- select *
from tempdb..fragmentedTables a 
join (select DatabaseName, Max (Rowidnum)  as MaxRowidnum from tempdb..fragmentedTables a where Updateability='Read_Only' group by DatabaseName) b
on a.DatabaseName= b.DatabaseName
and a.Rowidnum = b.MaxRowidnum


-- use sql engine to update Online/offline
declare @ReindexOnline varchar(200)
if SERVERPROPERTY ('EngineEdition')=3
begin
select @ReindexOnline='ON'
end
else
begin
select @ReindexOnline='OFF'
end


-- print final sql to execute

declare @SQLToExecute table
(
	Rowidnum  Int ,
	SQLToExecute varchar(8000),
	TimeoutValue varchar(200),
	flgBackupLog bit,
	flgUpdateability int ,
	DatabaseName varchar(200)
)

insert into @SQLToExecute (Rowidnum,SQLToExecute,TimeoutValue,flgBackupLog,flgUpdateability,DatabaseName)
select Rowidnum,
	case 
	when flg_Online = 0 and ((avg_fragmentation_in_percent > 30) or (avg_fragmentation_in_percent between 20 and 30 and page_count <=2000))
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ FullObjectName +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = OFF);' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)

	when flg_Online = 0 and (avg_fragmentation_in_percent between 20 and 30 and page_count >2000)
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ FullObjectName +' reorganize ;'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when flg_Online = 1 and ((avg_fragmentation_in_percent > 30) or (avg_fragmentation_in_percent between 20 and 30 and page_count <=2000))
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ FullObjectName +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = '+@ReindexOnline+');'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when flg_Online = 1 and (avg_fragmentation_in_percent between 20 and 30 and page_count >2000)
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ FullObjectName +' reorganize ;' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	else
		'alter index ['+ a.INDEX_Name +'] on '+ FullObjectName +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = OFF);' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	end
	,TimeoutValue,flgBackupLog,flgUpdateability, DatabaseName
-- select *
from tempdb..fragmentedTables a
order by a.database_id,TableName, INDEX_TYPE_DESC desc


declare @backupLogJob varchar(200)
if ('$LogBackupJobName' = '')
begin
    if exists (select name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1)
    select @backupLogJob=name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1
end
else
begin
    select @backupLogJob='$LogBackupJobName'
end

    select @backupLogJob= 'exec msdb..sp_start_job @job_name =''' + @backupLogJob + ''''

--select @backupLogJob = 'sqlcmd -S '+ @@SERVERNAME + ' -d master -E -Q `"' + @backupLogJob + '`"'  
--select @backupLogJob

select Rowidnum,SQLToExecute as sqlcmdToRun,TimeoutValue,flgBackupLog, @backupLogJob as LogbackupJob,flgUpdateability, DatabaseName from @SQLToExecute
order by Rowidnum

--select * from tempdb..fragmentedTables a 
/*

select *
FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (db_id('testdb'),NULL,NULL,NULL,NULL ) a

*/


"@



#$GenerateDBReindexCode | clip

#endregion


#region CollectreindexSyntax

    try
    {
        $ActivityName="`n`nGenerating fragmentation syntax"
        $ActivityName| write-PHLog  -echo -Logtype debug2
        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('GenerateDBReindexCode','Started')"
        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
        $CollectreindexSyntax=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $GenerateDBReindexCode -QueryTimeout 300   -Verbose 
        $CollectreindexSyntaxResult= $CollectreindexSyntax.sqlresult
        $CollectreindexSyntaxResult  | foreach { $_.sqlcmdToRun } | write-PHLog  -echo -Logtype debug2
        if (($CollectreindexSyntax.SQLTimeoutExpired -eq $true) -or ($CollectreindexSyntax.ExecuteSQLError -ne $null))
        {
            $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($GenerateDBReindexCode.Replace("'","''"))','$($CollectreindexSyntax.ExecuteSQLError)')"
            $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($CollectreindexSyntax.ExecuteSQLError) }
            ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
            "Generating fragmentation syntax failed, pleaes check why : $($CollectreindexSyntax.ExecuteSQLError) "| write-PHLog  -echo -Logtype Warning
        }
    }
    catch
    {
        "Error while gathering fragmentation syntax: $lServerName "| write-PHLog  -echo -Logtype Error
        $errMessage=$_.exception.message 
        $errMessage | write-PHLog  -echo -Logtype Error
        $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('GenerateDBReindexCode','$errMessage')"
        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
    }

#endregion


# lets start reindexing....
if ($CollectreindexSyntaxResult)
{
  foreach ($reindexStmt in $CollectreindexSyntaxResult) 
    {
    $Reindex_DatabaseName= $reindexStmt.DatabaseName

# Check if its readonly database, if yes enable for write....
	    if ($reindexStmt.flgUpdateability -eq 1)
	       {
            Try
            {
		        $ActivityName="Start Change Database Updateability to Read_Write " + (Get-Date -format "yyyy-M-d HH:mm:ss")  
                $ActivityName| write-PHLog  -echo -Logtype debug2
		        $ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_WRITE WITH NO_WAIT"
		        $ChangeDatabaseUpdateabilitycmdToRun  | write-PHLog  -echo -Logtype debug2
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','Started')"
                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
                $GetReindexRetValue=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ChangeDatabaseUpdateabilitycmdToRun -QueryTimeout 60  -Verbose 
                if (($GetReindexRetValue.SQLTimeoutExpired -eq $true) -or ($GetReindexRetValue.ExecuteSQLError -ne $null))
                {
                    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$($GetReindexRetValue.ExecuteSQLError)')"
                    $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($GetReindexRetValue.ExecuteSQLError) }
                    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                    "Changing Database to Read-Write Failed : $($GetReindexRetValue.ExecuteSQLError) "| write-PHLog  -echo -Logtype Warning
                }
            }
            catch
            {
                "Error while Change Database Updateability to Read_Write "| write-PHLog  -echo -Logtype Error
                $errMessage=$_.exception.message 
                $errMessage | write-PHLog  -echo -Logtype Error
                $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
                $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$errMessage')"
                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
            }



	       }
# Run the re-index command

        Try
        {
	            $ActivityName="Start running index " + (Get-Date -format "yyyy-M-d HH:mm:ss") +"`n" 
	            $ActivityName+=$reindexStmt | Out-String
                $ActivityName| write-PHLog  -echo -Logtype debug2
	            $LogBackupcmdToRun=$reindexStmt.LogbackupJob 
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($reindexStmt.sqlcmdToRun.Replace("'","''"))','Started')"
                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
                #$reindexStmt.TimeoutValue=2
                $GetSingleReindexRetValue=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $($reindexStmt.sqlcmdToRun) -QueryTimeout $($reindexStmt.TimeoutValue)   -Verbose 
                $GetSingleReindexRetValueResult= $GetSingleReindexRetValue.sqlresult
	            $GetSingleReindexRetValueResult  | write-PHLog  -echo -Logtype debug2
                if (($GetSingleReindexRetValue.SQLTimeoutExpired -eq $true) -or ($GetSingleReindexRetValue.ExecuteSQLError -ne $null))
                {
                    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($reindexStmt.sqlcmdToRun.Replace("'","''"))','$($GetSingleReindexRetValue.ExecuteSQLError)')"
                    $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($GetSingleReindexRetValue.ExecuteSQLError) }
                    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                    "Error while reindexing index : $($GetSingleReindexRetValue.ExecuteSQLError) "| write-PHLog  -echo -Logtype Error
                }

        }
        catch
        {

            "Error while fragmentating index "| write-PHLog  -echo -Logtype Error
            $errMessage=$_.exception.message 
            $errMessage | write-PHLog  -echo -Logtype Error
            $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
            $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($reindexStmt.sqlcmdToRun.Replace("'","''"))','$errMessage')"
            ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
        }



# Check if its readonly database, if yes disable for read_write ....
	    if ($reindexStmt.flgUpdateability -eq 2)
	       {
            Try
            {
		        $ActivityName="Start Change Database Updateability  to Read_Only" + (Get-Date -format "yyyy-M-d HH:mm:ss") 
                $ActivityName| write-PHLog  -echo -Logtype Debug2
		        $ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_ONLY WITH NO_WAIT"
		        $ChangeDatabaseUpdateabilitycmdToRun | write-PHLog  -echo -Logtype Debug2
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','Started')"
                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
                $GetChangeArchiveDBReadOnly=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ChangeDatabaseUpdateabilitycmdToRun -QueryTimeout 60   -Verbose 
                if (($GetChangeArchiveDBReadOnly.SQLTimeoutExpired -eq $true) -or ($GetChangeArchiveDBReadOnly.ExecuteSQLError -ne $null))
                {
                    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$($GetChangeArchiveDBReadOnly.ExecuteSQLError)')"
                    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                    $ManualAction="Pleaes manually enable database: $($Reindex_DatabaseName) to read only mode "
                    $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($GetChangeArchiveDBReadOnly.ExecuteSQLError) }
                    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$ManualAction')"
                    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                    "Error while changing db  $($Reindex_DatabaseName) to Read Only : $($GetChangeArchiveDBReadOnly.ExecuteSQLError) "| write-PHLog  -echo -Logtype Error
                    $ManualAction| write-PHLog  -echo -Logtype Error
                }

            }
            catch
            {
                "Error while  running log backup job "| write-PHLog  -echo -Logtype Error
                $errMessage=$_.exception.message 
                $errMessage | write-PHLog  -echo -Logtype Error
                $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
                $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$errMessage')"
                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
            }
		    Start-Sleep -s 2
    	    }
# take backup if flag is enabled
	    if ($reindexStmt.flgBackupLog -eq 1)
	       {
                if(!([string]::IsNullOrEmpty($LogBackupcmdToRun)))
                {

                    Try
                    {
		                $ActivityName="Start running log backup job" + (Get-Date -format "yyyy-M-d HH:mm:ss") 
                        $ActivityName | write-PHLog  -echo -Logtype Debug2
                        $LogBackupcmdToRun  | write-PHLog  -echo -Logtype Debug2
                        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($LogBackupcmdToRun.Replace("'","''"))','Started')"
                        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
                        $GetLogBackupRunValue=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $LogBackupcmdToRun -QueryTimeout 30   -Verbose -ErrorAction Stop
                        if (($GetLogBackupRunValue.SQLTimeoutExpired -eq $true) -or ($GetLogBackupRunValue.ExecuteSQLError -ne $null))
                        {
                            $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($LogBackupcmdToRun.Replace("'","''"))','$($GetLogBackupRunValue.ExecuteSQLError)')"
                            $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($GetLogBackupRunValue.ExecuteSQLError) }
                            ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                            "Error while running log backup job: $($GetLogBackupRunValue.ExecuteSQLError) "| write-PHLog  -echo -Logtype Error
                        }
$GetBackupJobStatus= @"
DECLARE @backupLogJob varchar(2000)
if ('$LogBackupJobName' = '')
begin
    if exists (select name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1)
    select @backupLogJob=name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1
end
else
begin
    select @backupLogJob='$LogBackupJobName'
end

if ('$LogBackupJobName' <> '')
begin

	DECLARE @job_id UNIQUEIDENTIFIER 
	select @job_id=job_id from msdb..sysjobs where name = @backupLogJob
	DECLARE @xp_results TABLE (job_id                UNIQUEIDENTIFIER NOT NULL,
			last_run_date         INT              NOT NULL,
			last_run_time         INT              NOT NULL,
			next_run_date         INT              NOT NULL,
			next_run_time         INT              NOT NULL,
			next_run_schedule_id  INT              NOT NULL,
			requested_to_run      INT              NOT NULL, -- BOOL
			request_source        INT              NOT NULL,
			request_source_id     sysname          COLLATE database_default NULL,
			running               INT              NOT NULL, -- BOOL
			current_step          INT              NOT NULL,
			current_retry_attempt INT              NOT NULL,
			job_state             INT              NOT NULL)

	DECLARE @can_see_all_running_jobs INT
	DECLARE @job_owner   sysname
	  SELECT @can_see_all_running_jobs = ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0)
	  IF (@can_see_all_running_jobs = 0)
	  BEGIN
		SELECT @can_see_all_running_jobs = ISNULL(IS_MEMBER(N'SQLAgentReaderRole'), 0)
	  END
	  SELECT @job_owner = SUSER_SNAME()

	IF ((@@microsoftversion / 0x01000000) >= 8) -- SQL Server 8.0 or greater
		INSERT INTO @xp_results
		EXECUTE master.dbo.xp_sqlagent_enum_jobs @can_see_all_running_jobs, @job_owner, @job_id

	select * from @xp_results
End
"@
#$GetBackupJobStatus|clip
                        $backupFinished=$true
                        while ($backupFinished)
                        {
                            $RetBackupJobStatus=ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $GetBackupJobStatus -QueryTimeout 30   -Verbose -ErrorAction Stop
                            if (($RetBackupJobStatus.SQLTimeoutExpired -eq $true) -or ($RetBackupJobStatus.ExecuteSQLError -ne $null))
                            {
                                $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($GetBackupJobStatus.Replace("'","''"))','$($RetBackupJobStatus.ExecuteSQLError)')"
                                $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($RetBackupJobStatus.ExecuteSQLError) }
                                ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                                "Error while getting log backup job status: $($RetBackupJobStatus.ExecuteSQLError) "| write-PHLog  -echo -Logtype Error
                            }
                            else
                            {
                                $RetBackupJobStatusResult= $RetBackupJobStatus.sqlresult
	                            $RetBackupJobStatusResult  | write-PHLog  -echo -Logtype debug2
                                if ($RetBackupJobStatusResult.running -eq $false)
                                {
                                    $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Log backup job finished, continuing with next reindexing job','Success')"
                                    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
                                    "Backup log job finished, continuing with next reindexing job" | write-PHLog  -echo -Logtype Debug2
                                    $backupFinished=$false
                                }
                                else
                                {
                                    "Backup log job not yet finished, waiting for 1 min to check again" | write-PHLog  -echo -Logtype Debug2
                                    Start-Sleep -s 60
                                }
                            }
                        }
                    }
                    catch
                    {
                        "Error while  running log backup job "| write-PHLog  -echo -Logtype Error
                        $errMessage=$_.exception.message 
                        $errMessage | write-PHLog  -echo -Logtype Error
                        $AppErrorCollection+=[pscustomobject] @{ErrorType="PS";ErrorActivityName=$ActivityName;ErrorMessage=$($errMessage) }
                        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($LogBackupcmdToRun.Replace("'","''"))','$errMessage')"
                        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                    }
                }
                else
                {
                        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Log backup job name could not be found, check backup job naming convention','Error')"
                        $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage="Log backup job name could not be found, check backup job naming convention" }
                        ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30  -Verbose 
                }
	       }

    }
}
else
{
    "No tables met reindex criteria"| write-PHLog  -echo -Logtype Success
    $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('No tables met reindex criteria','Success')"
    ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 
}

"Reindex process completed"| write-PHLog  -echo -Logtype Success
$SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Reindexing Completed','Success')"
ExecuteQueryV2 -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30  -Verbose 

}
else
{
    $ActivityName="Reindex process failed as db connection failed"
    $ActivityName| write-PHLog  -echo -Logtype Error
    $retPrepareIndexFragmentation.ExecuteSQLError| write-PHLog  -echo -Logtype Error
    $AppErrorCollection+=[pscustomobject] @{ErrorType="SQL";ErrorActivityName=$ActivityName;ErrorMessage=$($retPrepareIndexFragmentation.ExecuteSQLError) }
    Write-Error $retPrepareIndexFragmentation.ExecuteSQLError 
}


if ($AppErrorCollection)
{
    $body="<H2>Errors during reindex operation</H2>" 
    $AppErrorCollection | Select * -ExcludeProperty RowError, RowState, Table, ItemArray, HasErrors| ConvertTo-HTML  -head $a   -body $body| Out-File $ReindexStatusErrorSummary  
    throw "errors during execution pleaes check $ReindexStatusErrorSummary for more details"
}