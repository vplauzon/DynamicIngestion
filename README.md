# DynamicIngestion

Test a new ingestion policy algorithm

## Prepare database

```kql
.create table IngestTest(Timestamp:datetime, Partition:string, Node:string, Level:string, Component:string, EventText:string)

.create table Logs(Timestamp:datetime, Source:string, Level:string, EventText:string)

.alter table Logs policy streamingingestion enable
```