# Pathflow
Pathflows are used to get a view of how users are navigating within a session.Usually reperesented as Sunburst charts.  
Preparing data for this type chart requires partitioning,grouping and pivoting of data.

![Sunburst chart image](https://github.com/kkfaisal/pathflow-sql/blob/master/sunburst_chart.png)

## Input Table Structure  
Relevant fields :

event_id - Identifier for events  
event_name - Name of event  
session_id - Session identifier.  
date_time - Event happened time. 

## Data extracting 

#### In a high level, data extracting logic can be understood as follwing sequence of steps : 
  1. Partition data using session_id,order by time of event occured.Give number events.
  So event with number 10 will an event occured as 10th event in that session.  
  2. Find the number of starting event.For example if you want a pathflow with starting event as "Logged In" ,
  first Logged In event in that sessionwill be starting event  
  3. Group sessionId,starting_event_number and sequnce of events.This is to get multiple paths in same sessions counted.  
  4. Pivot the rows , so that events in path becoming coloumns.  
  5. Now find count of each path by groping on coloumns.    
  6. SQL given here has further data manipulations to make it ready for chart API. 
  
## Redshit and BigQuery  

  






