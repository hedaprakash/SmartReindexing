param (
    [string]$lServerName = "vmsfjiraproddb.advent.com",
    [string]$SqlUser = "svcvalidatesql",
    [string]$SqlPassword = "qqqqqq1!",
    [switch] $IgnoreReadOnlyDatabase
    )
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
#|: |09-27-2014 1.4     Prakash Heda  comments for presentation      |
#|: |04-27-2016 1.5     Prakash Heda  Updated debugging              |
#|{>\-------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |  
#+-------------------------------------------------------------------+  


#region CommonCode
#+-------------Common Code started-----------------------------------+    
CLS

$ScriptLocation=split-path -parent $MyInvocation.MyCommand.Path
$ScriptNameWithoutExt=[system.io.path]::GetFilenameWithoutExtension($MyInvocation.MyCommand.Path)

if ($lServerName.length -eq 0) {$lServerName  = gc env:computername}
$result= Test-Path C:\WINDOWS\Cluster\CLUSDB
switch ($result)
    {TRUE{$split = $lServerName.split("-");$lServerName = $split[0]}}

#$lServerName = "vSacAxDb28-1"

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
    if (Get-Module -ListAvailable | Where-Object { $_.name -eq "sqlps"})
    {  
	    if (!(Get-Module | Where-Object { $_.name -eq "sqlps"})) 
	    {  
	        Import-Module 'sqlps' –DisableNameChecking  | Out-Null 
	    } 
	}
	else
	{
		write-host "${runtime}: SQL Powershell Module is not installed on this server"
	}
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

$ScriptLocation

$runtime=Get-Date -format "yyyy-M-d HH:mm:ss"
$Logtime=Get-Date -format "yyyyMdHHmmss"
$LogPath=$ScriptLocation + "\pslogs\"
if(!(test-path $LogPath)){[IO.Directory]::CreateDirectory($LogPath)}
$ExecutionSummaryLogFile=$LogPath + $lServerName.Replace("-","_")  + "_" + $ScriptNameWithoutExt +  "_ExecutionSummary_" + $Logtime + ".html"
"Starting "| write-PHLog -Logtype Debug2


$LogName= $LogPath + $lServerName.Replace("-","_") + "_" +$Logtime + ".log"
$SQLGenerateDBReindex= $ScriptLocation+"\GenerateDBReindex.sql"
$SQLGenerateDBReindex
$SQLGenerateDBReindexOutput= $LogPath + $lServerName.Replace("-","_") +  "_" +"GenerateDBReindex" + "_" +$Logtime + ".txt"
$SQLcmdbatfile= $ScriptLocation+ "\pslogs\" +"executeReindex" + "_" +$Logtime + ".bat"

$startSumamry="`r`nLogName: $ExecutionSummaryLogFile`r`nScriptLocation: $ScriptLocation`r`n"

$startSumamry | write-PHLog -Logtype Debug2

#endregion

$ErrorActionPreference = "STOP"

#region PrepareReinDexStats
$reIndexruntime_Collect= "Start collecting index stats " + (Get-Date -format "yyyy-M-d HH:mm:ss")
$reIndexruntime_Collect | write-PHLog -Logtype Debug2

if ($IgnoreReadOnlyDatabase.IsPresent){$IgnoreReadOnlyDatabaseflg=1} else {$IgnoreReadOnlyDatabaseflg=0}

$IgnoreReadOnlyDatabaseflg=1

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

if (0=$IgnoreReadOnlyDatabaseflg)
select name from sys.databases where name not in ( 'master','Model') 
else
select name from sys.databases where name not in ( 'master','Model') and is_read_only = 0


"@



try
{
    "Preparing index fragmentation collection table: $lServerName "| write-PHLog  -echo -Logtype debug2
    $retPrepareIndexFragmentation=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $PrepareIndexFragmentation -QueryTimeout 60 -Username $SqlUser -Password $SqlPassword -Verbose  
    $retPrepareIndexFragmentation |  write-PHLog  -echo -Logtype debug2
    $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($PrepareIndexFragmentation.Replace("'","''"))','Started')"
    Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 

}
catch
{

    "Error while preparing index fragmentation collection table: $lServerName "| write-PHLog  -echo -Logtype Error
    $_.exception.message | write-PHLog  -echo -Logtype Error
    $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($PrepareIndexFragmentation.Replace("'","''"))','$errMessage')"
    Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 

}


foreach ($dbname in $retPrepareIndexFragmentation.name)
{

    "Collecting fragmentation information from $dbname "| write-PHLog  -echo -Logtype debug2
    $CollectDBIndexFragmentation= @"
        Insert into  tempdb..CollectFragmentationDetails908  select * FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (db_id('$dbname'),NULL,NULL,NULL,NULL ) a 
"@
    $CollectDBIndexFragmentation

    Try 
    {
        $retDBIndexFragmentation=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $CollectDBIndexFragmentation -QueryTimeout 300 -Username $SqlUser -Password $SqlPassword -Verbose  
        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($CollectDBIndexFragmentation.Replace("'","''"))','Success')"
        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
    }
    catch
    {

        "Error while collecting index fragmentation collection table: $lServerName..$dbname "| write-PHLog  -echo -Logtype Error
        $errMessage=$_.exception.message 
        $errMessage | write-PHLog  -echo -Logtype Error
        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($CollectDBIndexFragmentation.Replace("'","''"))','$errMessage')"
        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
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
, convert(varchar(500),'') as Objectname,0 as fill_factor,60+(CEILING (page_count/1000)) as TimeOutvalue
into ##tmpFragmentationDetails908_2
from tempdb..CollectFragmentationDetails908
where AVG_FRAGMENTATION_IN_PERCENT > 20 or INDEX_TYPE_DESC = 'HEAP'


delete from ##tmpFragmentationDetails908_2 where AVG_FRAGMENTATION_IN_PERCENT < 15 or INDEX_TYPE_DESC = 'HEAP' -- or page_count<20

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
	objectNAME varchar(2000),
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
	objectname = b.objectname,FILL_FACTOR =b.FILL_FACTOR 
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
	db_name(database_id),CAST(OBJECT_NAME(a.OBJECT_ID,database_id) AS VARCHAR(200))
	,index_id,index_type_desc,avg_fragmentation_in_percent,page_count
	,Objectname
	,FILL_FACTOR,isnull(nullif(FILL_FACTOR,0),90),schemaName,object_id,database_id,flg_Online,TimeOutvalue,0,0,convert(varchar(200),DATABASEPROPERTYEX ( db_name(database_id) , 'Updateability' ) ), 0 
from ##tmpFragmentationDetails908_2 a
where  INDEX_TYPE_DESC <> 'HEAP'
and page_count>40

-- SELECT * FROM TEMPDB..fragmentedTables where databaseName='testdb'

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
group by SubtotalPages/50000)



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
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = OFF);' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)

	when flg_Online = 0 and (avg_fragmentation_in_percent between 20 and 30 and page_count >2000)
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' reorganize ;'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when flg_Online = 1 and ((avg_fragmentation_in_percent > 30) or (avg_fragmentation_in_percent between 20 and 30 and page_count <=2000))
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = '+@ReindexOnline+');'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when flg_Online = 1 and (avg_fragmentation_in_percent between 20 and 30 and page_count >2000)
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' reorganize ;' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	else
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = OFF);' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	end
	,TimeoutValue,flgBackupLog,flgUpdateability, DatabaseName
from tempdb..fragmentedTables a
order by a.database_id,TableName, INDEX_TYPE_DESC desc


declare @backupLogJob varchar(200)
if exists (select name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1)
select @backupLogJob=name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup','DBA_BackupDB.Logsbackup') and enabled=1
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

# $GenerateDBReindexCode | clip

#endregion


#region CollectreindexSyntax

    try
    {
        "Generating fragmentation syntax"| write-PHLog  -echo -Logtype debug2
        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('GenerateDBReindexCode','Started')"
        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
        $CollectreindexSyntax=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $GenerateDBReindexCode -QueryTimeout 300  -Username $SqlUser -Password $SqlPassword -Verbose 
        $CollectreindexSyntax  | foreach { $_.sqlcmdToRun } | write-PHLog  -echo -Logtype debug2
    }
    catch
    {

        "Error while gathering fragmentation syntax: $lServerName "| write-PHLog  -echo -Logtype Error
        $errMessage=$_.exception.message 
        $errMessage | write-PHLog  -echo -Logtype Error
        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('GenerateDBReindexCode','$errMessage')"
        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
    }

#endregion


# lets start reindexing....
if ($CollectreindexSyntax)
{
  foreach ($reindexStmt in $CollectreindexSyntax) 
    {
    $Reindex_DatabaseName= $reindexStmt.DatabaseName

# Check if its readonly database, if yes enable for write....
	    if ($reindexStmt.flgUpdateability -eq 1)
	       {
            Try
            {
		        "Start Change Database Updateability to Read_Write " + (Get-Date -format "yyyy-M-d HH:mm:ss")  | write-PHLog  -echo -Logtype debug2
		        $ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_WRITE WITH NO_WAIT"
		        $ChangeDatabaseUpdateabilitycmdToRun  | write-PHLog  -echo -Logtype debug2
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','Started')"
                Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                $GetReindexRetValue=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ChangeDatabaseUpdateabilitycmdToRun -QueryTimeout 60  -Username $SqlUser -Password $SqlPassword -Verbose 
            }
            catch
            {
                "Error while Change Database Updateability to Read_Write "| write-PHLog  -echo -Logtype Error
                $errMessage=$_.exception.message 
                $errMessage | write-PHLog  -echo -Logtype Error
                $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$errMessage')"
                Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
            }



	       }
# Run the re-index command

        Try
        {
	            $reindexStmt  | write-PHLog  -echo -Logtype debug2
	            "Start running index " + (Get-Date -format "yyyy-M-d HH:mm:ss")  | write-PHLog  -echo -Logtype debug2
	            $LogBackupcmdToRun=$reindexStmt.LogbackupJob 
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($reindexStmt.sqlcmdToRun.Replace("'","''"))','Started')"
                Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                $GetReindexRetValue=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $($reindexStmt.sqlcmdToRun) -QueryTimeout $($reindexStmt.TimeoutValue)  -Username $SqlUser -Password $SqlPassword -Verbose 
	            $GetReindexRetValue  | write-PHLog  -echo -Logtype debug2
        }
        catch
        {

            "Error while fragmentating index "| write-PHLog  -echo -Logtype Error
            $errMessage=$_.exception.message 
            $errMessage | write-PHLog  -echo -Logtype Error
            $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($reindexStmt.sqlcmdToRun.Replace("'","''"))','$errMessage')"
            Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
        }



# Check if its readonly database, if yes disable for read_write ....
	    if ($reindexStmt.flgUpdateability -eq 2)
	       {
            Try
            {
		        "Start Change Database Updateability  to Read_Only" + (Get-Date -format "yyyy-M-d HH:mm:ss") | write-PHLog  -echo -Logtype Debug2
		        $ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_ONLY WITH NO_WAIT"
		        $ChangeDatabaseUpdateabilitycmdToRun | write-PHLog  -echo -Logtype Debug2
                $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','Started')"
                Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                $GetReindexRetValue=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ChangeDatabaseUpdateabilitycmdToRun -QueryTimeout 60  -Username $SqlUser -Password $SqlPassword -Verbose 

            }
            catch
            {
                "Error while  running log backup job "| write-PHLog  -echo -Logtype Error
                $errMessage=$_.exception.message 
                $errMessage | write-PHLog  -echo -Logtype Error
                $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($ChangeDatabaseUpdateabilitycmdToRun.Replace("'","''"))','$errMessage')"
                Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
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
		                "Start running log backup job" + (Get-Date -format "yyyy-M-d HH:mm:ss") | write-PHLog  -echo -Logtype Debug2
                        $LogBackupcmdToRun  | write-PHLog  -echo -Logtype Debug2
                        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($LogBackupcmdToRun.Replace("'","''"))','Started')"
                        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                        $GetReindexRetValue=Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $LogBackupcmdToRun -QueryTimeout 30  -Username $SqlUser -Password $SqlPassword -Verbose -ErrorAction Stop
                        "Waiting for 30 sec"  | write-PHLog  -echo -Logtype Debug2
                        $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Waiting for 30 sec','Success')"
                        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
            		    Start-Sleep -s 30

                    }
                    catch
                    {
                        "Error while  running log backup job "| write-PHLog  -echo -Logtype Error
                        $errMessage=$_.exception.message 
                        $errMessage | write-PHLog  -echo -Logtype Error
                        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('$($LogBackupcmdToRun.Replace("'","''"))','$errMessage')"
                        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                    }
                }
                else
                {
                        $ErrMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Log backup job name could not be found, check backup job naming convention','Error')"
                        Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $ErrMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 
                }
	       }

    }
}
else
{
    "No tables met reindex criteria"| write-PHLog  -echo -Logtype Success
    $SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('No tables met reindex criteria','Success')"
    Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 

}

"Reindex process completed"| write-PHLog  -echo -Logtype Success
$SuccessMsgToLog= "INSERT INTO msdb..tbl_indexRebuild_Log(indexRebuildCommand,returnValue) VALUES('Reindexing Completed','Success')"
Invoke-Sqlcmd -ServerInstance $lServerName -database "master" -Query $SuccessMsgToLog -QueryTimeout 30 -Username $SqlUser -Password $SqlPassword -Verbose 

