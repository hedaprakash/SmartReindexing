#+-------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |    
#|{>/-------------------------------------------------------------\<}| 
#|: | Script Name: SmartReindexing.ps1                              		 | 
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
#|{>\-------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |  
#+-------------------------------------------------------------------+  


#region CommonCode
#+-------------Common Code started-----------------------------------+    
CLS

$ScriptLocation=split-path -parent $MyInvocation.MyCommand.Path

$ComputerNameParam  = gc env:computername
$result= Test-Path C:\WINDOWS\Cluster\CLUSDB
switch ($result)
    {TRUE{$split = $ComputerNameParam.split("-");$ComputerNameParam = $split[0]}}

#$ComputerNameParam = "vSacAxDb28-1"

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




set-location "C:\" -PassThru | Out-Null 
set-location $ScriptLocation -PassThru | Out-Null 

$ScriptLocation

$runtime=Get-Date -format "yyyy-M-d HH:mm:ss"
$Logtime=Get-Date -format "yyyyMdHHmmss"
$LogPath=$ScriptLocation + "\pslogs\"
if(!(test-path $LogPath)){[IO.Directory]::CreateDirectory($LogPath)}


#+-------------Common Code eded-----------------------------------+    
#endregion

#region ConfigureVariables

$LogName= $LogPath + $ComputerNameParam.Replace("-","_") + "_" +$Logtime + ".log"
$SQLGenerateDBReindex= $ScriptLocation+"\GenerateDBReindex.sql"
$SQLGenerateDBReindex
$SQLGenerateDBReindexOutput= $LogPath + $ComputerNameParam.Replace("-","_") +  "_" +"GenerateDBReindex" + "_" +$Logtime + ".txt"
$SQLcmdbatfile= $ScriptLocation+ "\pslogs\" +"executeReindex" + "_" +$Logtime + ".bat"

$startSumamry="`r`nLogName: $LogName`r`nScriptLocation: $ScriptLocation`r`n"

Write-Host $startSumamry

Add-Content -Path $LogName -Value $startSumamry

#endregion

#region GenerateDBReindexCode
# Code to generate reindex syntax

$GenerateDBReindexCode= @"


/*
#+-------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |    
#|{>/-------------------------------------------------------------\<}| 
#|: | Script Name: GenerateReindexCode.sql                           | 
#|: | Author:  Prakash Heda                                          | 
#|: | Email:   Pheda@advent.com	 Blog:www.sqlfeatures.com   		 |
#|: | Purpose: Automatically generrate reindex code	                 |
#|: | 							 	 								 |
#|: |Date       Version ModifiedBy    Change 						 |
#|: |05-16-2012 1.0     Prakash Heda  Initial version                |
#|: |07-25-2013 1.1     Prakash Heda  Working for SQL 2012           |
#|: |05-02-2014 1.2     Hua Yu  Optional Support for Read Only       |
#|: |                   databases                                    |
#|: |07-25-2014 1.3     Prakash Heda  Working for SQL 2014           |
#|{>\-------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = |  
#+-------------------------------------------------------------------+  
*/

set nocount on
GO

USE [tempdb]
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

insert into  tempdb..CollectFragmentationDetails908
select *
FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (NULL,NULL,NULL,NULL,NULL ) a

/*
select *
FROM   SYS.DM_DB_INDEX_PHYSICAL_STATS (db_id('testdb'),NULL,NULL,NULL,NULL ) a

*/

-- Collect online vs offline tables
-- truncate table tempdb..fragmentedTables
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
object_id,db_id(''?'') from ?.sys.columns c 
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
group by SubtotalPages/4000)



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
	when avg_fragmentation_in_percent between 20 and 30  and page_count >2000
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' reorganize ;' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when avg_fragmentation_in_percent > 30 or page_count <2000
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = '+@ReindexOnline+');'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	else
	''
	end
	,TimeoutValue,flgBackupLog,flgUpdateability, DatabaseName
from tempdb..fragmentedTables a
where  flg_Online = 1
order by a.database_id,TableName, INDEX_TYPE_DESC desc

insert into @SQLToExecute (Rowidnum,SQLToExecute,TimeoutValue,flgBackupLog,flgUpdateability,DatabaseName)
select Rowidnum,
	case 
	when avg_fragmentation_in_percent between 20 and 30
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' reorganize ;'  + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	when avg_fragmentation_in_percent > 30
	then 
		'alter index ['+ a.INDEX_Name +'] on '+ db_name(a.database_id) +'.' + a.SchemaName + '.' + object_name(a.object_id,a.database_id) +' REBUILD WITH (FILLFACTOR = '+ convert(varchar(200),a.EXPECTED_FILL_FACTOR)+', SORT_IN_TEMPDB = ON,STATISTICS_NORECOMPUTE = ON, ONLINE = OFF);' + '--Current fragmentation level: ' + convert(VARCHAR(200),AVG_FRAGMENTATION_IN_PERCENT) + ' Page Count: ' + convert(VARCHAR(200),page_count)
	else
	''
	end
	,TimeoutValue,flgBackupLog,flgUpdateability, DatabaseName
from tempdb..fragmentedTables a
where  flg_Online = 0
order by a.database_id 

declare @backupLogJob varchar(200)
if exists (select name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup') and enabled=1)
select @backupLogJob=name from msdb..sysjobs where name in ('DBA:Backup All Tlogs','DBA_BackupDB.LogBackup') and enabled=1
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

#endregion

$ErrorActionPreference = "silentlycontinue"

#region CollectReinDexStats
$reIndexruntime_Collect= "Start collecting index stats " + (Get-Date -format "yyyy-M-d HH:mm:ss")
Add-Content -Path $LogName -Value $reIndexruntime_Collect


$CollectreindexSyntax=Invoke-Sqlcmd -ServerInstance $ComputerNameParam -database "master" -Query $GenerateDBReindexCode -QueryTimeout 60000  -Verbose  -ErrorAction Continue 

#$CollectreindexSyntax=Invoke-Sqlcmd -ServerInstance $ComputerNameParam -database "master" -InputFile $SQLGenerateDBReindex  -QueryTimeout 600000  -Verbose  -ErrorAction Continue 
$CollectreindexSyntax
$CollectreindexSyntax  | foreach { $_.sqlcmdToRun } | Out-File  $SQLGenerateDBReindexOutput 
#.$SQLGenerateDBReindexOutput

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
		$ChangeDatabaseUpdateability= "Start Change Database Updateability to Read_Write " + (Get-Date -format "yyyy-M-d HH:mm:ss")
		Add-Content -Path $LogName -Value $ChangeDatabaseUpdateability
		$ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_WRITE WITH NO_WAIT"
		$ChangeDatabaseUpdateabilitycmdToRun = "sqlcmd -S $ComputerNameParam -d master -E -Q `"" + $ChangeDatabaseUpdateabilitycmdToRun + "`""
		$ChangeDatabaseUpdateabilitycmdToRun
		Add-Content -Path $LogName -Value $ChangeDatabaseUpdateabilitycmdToRun
		$GetChangeDatabaseUpdateabilitycmdToRun=Invoke-Expression $ChangeDatabaseUpdateabilitycmdToRun
		$GetChangeDatabaseUpdateabilitycmdToRun
		Add-Content -Path $LogName -Value $GetChangeDatabaseUpdateabilitycmdToRun
		Start-Sleep -s 2
	   }
	
# Run the re-index command
	$reindexStmt
	$indexTimeoutValue = $reindexStmt.TimeoutValue
	$Indexruntime= "Start running index " + (Get-Date -format "yyyy-M-d HH:mm:ss")
	$cmdToRun=$reindexStmt.sqlcmdToRun 
	$LogBackupcmdToRun=$reindexStmt.LogbackupJob 
	Add-Content -Path $LogName -Value $Indexruntime
	$cmdToRun = "sqlcmd -S $ComputerNameParam -d master -t $indexTimeoutValue -E -Q `"" + $cmdToRun + "`""
	$LogName
	Add-Content -Path $LogName -Value $cmdToRun
	$GetReindexRetValue=Invoke-Expression $cmdToRun
	
	Add-Content -Path $LogName -Value $GetReindexRetValue

# Check if its readonly database, if yes disable for read_write ....
	if ($reindexStmt.flgUpdateability -eq 2)
	   {
		$ChangeDatabaseUpdateability= "Start Change Database Updateability  to Read_Only" + (Get-Date -format "yyyy-M-d HH:mm:ss")
		Add-Content -Path $LogName -Value $ChangeDatabaseUpdateability
		$ChangeDatabaseUpdateabilitycmdToRun = "if db_id('$Reindex_DatabaseName') is not null ALTER DATABASE [$Reindex_DatabaseName] SET  READ_ONLY WITH NO_WAIT"
		$ChangeDatabaseUpdateabilitycmdToRun = "sqlcmd -S $ComputerNameParam -d master -E -Q `"" + $ChangeDatabaseUpdateabilitycmdToRun + "`""
		$ChangeDatabaseUpdateabilitycmdToRun
		Add-Content -Path $LogName -Value $ChangeDatabaseUpdateabilitycmdToRun
		$GetChangeDatabaseUpdateabilitycmdToRun=Invoke-Expression $ChangeDatabaseUpdateabilitycmdToRun
		$GetChangeDatabaseUpdateabilitycmdToRun
		Add-Content -Path $LogName -Value $GetChangeDatabaseUpdateabilitycmdToRun
		Start-Sleep -s 2
    	}
# take backup if flag is enabled
	if ($reindexStmt.flgBackupLog -eq 1)
	   {
		$Logbackupruntime= "Start running log backup " + (Get-Date -format "yyyy-M-d HH:mm:ss")
		Add-Content -Path $LogName -Value $Logbackupruntime
		#$LogBackupcmdToRun | Out-File $SQLcmdbatfile
		$LogBackupcmdToRun = "sqlcmd -S $ComputerNameParam -d master -E -Q `"" + $LogBackupcmdToRun + "`""
		Add-Content -Path $LogName -Value $LogBackupcmdToRun
		$GetLogBackupRetValue=Invoke-Expression $LogBackupcmdToRun
		$GetLogBackupRetValue
		Add-Content -Path $LogName -Value $GetLogBackupRetValue
		Start-Sleep -s 20
	   }

    }
}
	Add-Content -Path $LogName -Value "No Tables to be Re-Indexed."
#.$LogName
