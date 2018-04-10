# Path-flow
Path-flows are used to get a view of how users are navigating within a session.Usually represented as Sunburst charts.  
Preparing data for this type chart requires partitioning,grouping and pivoting of data.

![Sunburst chart image](https://github.com/kkfaisal/pathflow-sql/blob/master/sunburst_chart.png)

## Input Table Structure  
Relevant fields :

event_id - Identifier for events  
event_name - Name of event  
session_id - Session identifier.  
date_time - Event happened time. 

## Data extracting 

#### In a high level, data extracting logic can be understood as following sequence of steps : 
  1. Partition data using session_id,order by time of event occurred.Give number events.
  So event with number 10 will an event occurred as 10th event in that session.  
  2. Find the number of starting event.For example if you want a path-flow with starting event as "Logged In" ,
  first Logged In event in that session will be starting event  
  3. Group sessionId,starting_event_number and sequence of events.This is to get multiple paths in same sessions counted.  
  4. Pivot the rows , so that events in path becoming columns.  
  5. Now find count of each path by groping on columns.    
  6. SQL given here has further data manipulations to make it ready for chart API. 
  
## Redshift and BigQuery  
The SQL was originally written for Redshift.As handy tool which improves readability of SQL,we preferred to use Window functions in Redshift.ANd it performed well.
There are 11 window functions in [pathflow_redshift.sql](https://github.com/kkfaisal/pathflow-sql/blob/master/pathflow_redshift.sql)  

While doing  benchmarking of BigQuery original path-flow SQL running on Redshift failed with resource exhausted error, even for one month data.So we re-wrote path-flow SQL to use more `group by`s and joins instead of window function.As you can see only two window functions are present in [pathflow_bq.sql](https://github.com/kkfaisal/pathflow-sql/blob/master/pathflow_bq.sql). It worked well in BigQuery , faster than Redshift.  

Interestingly, we try running SQL which ran on BigQuery in Redshift.It was slower than running original SQL on Redshift. 

Might be Redshift and BigQuery using different SQL optimization techniques.
