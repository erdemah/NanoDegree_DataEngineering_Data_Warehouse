<h1>About The Project</h1>
<p>Sparkify has grown in the business and they want to move from a single database to data warehouse using AWS' Redshift service. The main motivation behind this move is that they want to continuously watch their user and musician segments in order to get valuable insights from their data. Moreover, sales and marketing departments demand to use more data and new functionalities result in increased amount of data formats. In order to handle these tasks, we need to move away from a single database to a datawarehouse.</p>
<h2>Overview</h2>
<p>The schema has been created based on the raw data in the s3 bucket. First, log and song data files residing in s3 bucket as a json format have been loaded into Redshift without any transformations using Python's psycopg2 library - a popular posgreSQL database adapter which is called staging area (EXTRACT). Then, we transformed the the data logically from the staging area (TRANSFORM) and loaded into 5 tables (1 fact and 4 dimensional) for the reporting team to use (LOAD).</p>
<h2>How to run</h2>
<p>Run etl.py to initiate the program. What etl.py does is first it creates the necessary components such as creating a new IAM role and attaching the policy. Having obtained the role arn, we can create the redshift cluster. When the cluster becomes available, we connect to the data warehouse from its endpoint. We then follow these steps to create and insert the necessary tables:</p>
<p>Drop Tables</p>
<p>Create Tables</p>
<p>Load Tables</p>
<p>Insert Tables</p>
<p>After the insertion is complete, we can run queries in the redshift cluster. The cluster will be teared down within 5 minutes. All these activities such as creating IAM roles, cluster and tearing down take place in redshiftbuilder.py as an infrastructre as code</p>
<h2>Insight</h2>
<p><b>What are the top songs that were listened the most on the weekends in 2018?</b></p>
<p>SELECT a.name, s.title, COUNT(*) as number_of_times 
<p> FROM songplay_table as sp </p> 
<p>LEFT JOIN time_table as t on sp.start_time = t.start_time</p>
<p>LEFT JOIN artist_table as a on a.artist_id = sp.artist_id</p>
<p>LEFT JOIN song_table as s on s.song_id = sp.song_id</p>
<p>WHERE (t.weekday = 5 or t.weekday = 6) AND t.year = 2018</p>
<p>GROUP BY a.name, s.title</p>
<p>ORDER BY number_of_times DESC;</p>


