# MMT Assignment (Data Feed for Drop-off Notifications)

NOTE: Please let me know once this assignment is checked, I will make it private after that. 
[Click here](https://github.com/ramands/mmt-data-feed/blob/master/dropoff_data_feed.py) to check the code.

### Step 1: Create an EMR Cluster with Hive, Spark, and Zepplin.
------
Hive is used as the main data store to fetch the click stream and to save the data feed.
Spark is used to process the data and create a data feed. 
Zepplin notebook is used to write and test out the code

```
aws emr create-cluster --name="HiveExample" --release-label emr-5.21.0 \
--applications Name=Hive Name=Hadoop Name=Spark Name=Zeppelin \
--use-default-roles --instance-type m4.4xlarge --instance-count 1 --ec2-attributes KeyName=stage-data-pipeline \
--profile=development
```

### Step 2. Create `funnel_clickstreams` table in hive. This will store the clickstream. 
------
```
CREATE TABLE IF NOT EXISTS funnel_clickstreams ( 
    userid String, 
    origin String, 
    destination String, 
    departure_date date, 
    page_name String, 
    search_date_time timestamp 
);
```

### Step 3. Add sample data to the table created in the previous step.
------
```
INSERT INTO funnel_clickstreams VALUES
('u1', 'DEL', 'BLR', '2019-08-25', 'listing', '2019-08-25 10:00:00'),
('u1', 'DEL', 'BLR', '2019-03-25', 'listing', '2019-03-05 10:00:00'),
('u1', 'DEL', 'BLR', '2019-03-25', 'review', '2019-03-05 10:00:00'),
('u1', 'DEL', 'BLR', '2019-03-25', 'payments', '2019-03-05 10:01:00'),
('u1', 'DEL', 'BLR', '2019-03-24', 'review', '2019-03-10 16:01:00'),
('u1', 'DEL', 'BLR', '2019-03-25', 'listing', '2019-03-10 16:01:00'),
('u1', 'DEL', 'BLR', '2019-03-24', 'listing', '2019-03-10 16:00:00'),
('u1', 'DEL', 'BLR', '2019-03-25', 'review', '2019-03-10 16:01:00'),
('u1', 'DEL', 'GOI', '2019-12-25', 'payments', '2019-03-10 16:03:00'),
('u1', 'DEL', 'GOI', '2019-12-25', 'listing', '2019-03-10 16:02:00'),
('u1', 'DEL', 'GOI', '2019-12-25', 'review', '2019-03-10 16:03:00'),
('u1', 'DEL', 'GOI', '2019-12-25', 'thankyou', '2019-03-10 16:04:00'),
('u1', 'DEL', 'BLR', '2019-03-12', 'listing', '2019-03-10 20:00:00'),
('u1', 'DEL', 'BLR', '2019-03-12', 'payments', '2019-03-10 20:01:00'),
('u2', 'DEL', 'BOM', '2019-03-25', 'listing', '2019-03-10 20:01:00'),
('u2', 'DEL', 'BOM', '2019-03-12', 'listing', '2019-03-11 20:02:00'),
('u1', 'DEL', 'BLR', '2019-03-12', 'review', '2019-03-10 20:01:00'),
('u1', 'DEL', 'BLR', '2018-08-25', 'listing', '2018-08-25 10:00:00'),
('u1', 'DEL', 'BLR', '2018-03-25', 'listing', '2018-03-05 10:00:00'),
('u1', 'DEL', 'BLR', '2018-03-25', 'review', '2018-03-05 10:00:00'),
('u1', 'DEL', 'BLR', '2018-03-25', 'payments', '2018-03-05 10:01:00'),
('u1', 'DEL', 'BLR', '2018-03-24', 'review', '2018-03-10 16:01:00'),
('u1', 'DEL', 'BLR', '2018-03-25', 'listing', '2018-03-10 16:01:00'),
('u1', 'DEL', 'BLR', '2018-03-24', 'listing', '2018-03-10 16:00:00'),
('u1', 'DEL', 'BLR', '2018-03-25', 'review', '2018-03-10 16:01:00'),
('u1', 'DEL', 'GOI', '2018-12-25', 'payments', '2018-03-10 16:03:00'),
('u1', 'DEL', 'GOI', '2018-12-25', 'listing', '2018-03-10 16:02:00'),
('u1', 'DEL', 'GOI', '2018-12-25', 'review', '2018-03-10 16:03:00'),
('u1', 'DEL', 'GOI', '2018-12-25', 'thankyou', '2018-03-10 16:04:00'),
('u1', 'DEL', 'BLR', '2018-03-12', 'listing', '2018-03-10 20:00:00'),
('u1', 'DEL', 'BLR', '2018-03-12', 'payments', '2018-03-10 20:01:00'),
('u2', 'DEL', 'BOM', '2018-03-25', 'listing', '2018-03-10 20:01:00'),
('u2', 'DEL', 'BOM', '2018-03-12', 'listing', '2018-03-11 20:02:00'),
('u1', 'DEL', 'BLR', '2018-03-12', 'review', '2018-03-10 20:01:00');
```