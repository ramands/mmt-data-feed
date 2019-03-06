# NOTE: This code was written in Zepplin notebook on an EMR cluster. 
# By default, sparkcontext (sc) is initialised, hence, line number 13 is commented out.


# Make necessary imports
import datetime
from itertools import chain
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import create_map, lit, max

# Initialise sparkcontext and hivecontext
# sc = SparkContext("local", "First App") # Check NOTE 👆
hive_context = HiveContext(sc)
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# Create a data frame with the raw data available in Hive
clickstream_funnel_df = hive_context.table("default.funnel_clickstreams")

# Assign a code to the various pages. This code will also act as the 'likeliness' of the user getting converted. 
# A higher numbered code implies that the user is more likely to buy the ticket.
page_code = {'listing': 1, 'review': 2, 'payments': 3, 'thankyou': 4}
mapping_expr = create_map([lit(x) for x in chain(*page_code.items())])

# Add a new column 'conversion_likeliness' to the dataframe and filter out past searches
conversion_likeliness_data_df = clickstream_funnel_df.withColumn('conversion_likeliness', mapping_expr[clickstream_funnel_df['page_name']])
conversion_likeliness_data_df = conversion_likeliness_data_df.filter(conversion_likeliness_data_df.departure_date>current_date).alias('df')

# Select unique searches by the user and fetch its max 'likeliness' to be converted.  
unique_searches_df = conversion_likeliness_data_df. \
		groupBy('userid', 'origin', 'destination', 'departure_date'). \
		agg(max(conversion_likeliness_data_df['conversion_likeliness']).alias('conversion_likeliness'))

# Remove the searches that have already been converted successfully.
unique_searches_df = unique_searches_df.filter(unique_searches_df.conversion_likeliness != lit(4)).alias('unique_searches_df')

# Join the dataframes and remove unwanted columns. This is the data feed required
drop_off_data_feed = unique_searches_df. \
		join(conversion_likeliness_data_df,  \
			(unique_searches_df.userid == conversion_likeliness_data_df.userid) &  \
			(unique_searches_df.origin == conversion_likeliness_data_df.origin) &  \
			(unique_searches_df.destination == conversion_likeliness_data_df.destination) &  \
			(unique_searches_df.departure_date == conversion_likeliness_data_df.departure_date) &  \
			(unique_searches_df.conversion_likeliness == conversion_likeliness_data_df.conversion_likeliness) \
		). \
		drop(unique_searches_df.userid). \
		drop(unique_searches_df.origin). \
		drop(unique_searches_df.destination). \
		drop(unique_searches_df.departure_date). \
		drop(unique_searches_df.conversion_likeliness). \
		withColumnRenamed("page_name", "dropoff_page")

# Save the data feed back to a data store. Here, the data is getting saved in a hive table.
drop_off_data_feed.write.mode('overwrite').saveAsTable("drop_off_data_feed")