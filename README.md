# SmartReindexing
Super Smart Universal Reindexing script supporting SQL 2005 onwards

SmartReindexing.ps1 - Script to run from local or network computer

SmartReindexing_TestJob.sql - Test job to schedule reindexing job

Full Video explaining script: (this is for V 1.0)

https://www.youtube.com/watch?v=rTJhJy51BIE&feature=youtu.be

License: The SQL Server Maintenance Solution is free.


Destinctive Features:

Reindex only fragmented tables

Takes log backups in between to ensure logfile do not get filled up

Timeout automatically based on table size to avoid long blockings created by Index jobs

Automatically update fill factor to 90 if not specified for tables with fragmentation

Detailed execution status is logged into msdb..tbl_indexRebuild_Log 

Check SmartReindexing - The quest of ultimate automation.docx for usage details

