from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def broadcast_join_maps(spark):
    # Create Maps and Matches DFs
    matches_df = spark.read.csv('../../data/matches.csv', header=True, inferSchema=True)
    maps_df = spark.read.csv('../../data/maps.csv', header=True, inferSchema=True)
    
    # Broadcast join maps_Df with matches_df
    matches_maps_df = matches_df.join(broadcast(maps_df), 'mapid')
    
    return matches_maps_df
    
def broadcast_join_medals(spark):
    # Create Medals and Matches Players DataFrames
    medals_df = spark.read.csv('../../data/medals.csv', header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv('../../data/medals_matches_players.csv', header=True, inferSchema=True)
    
    # Broadcast join medals_df with medal_matches_players_df
    medals_player_df = medals_matches_players_df.join(broadcast(medals_df), 'medal_id')

    return medals_player_df
    
def bucket_join_matches(spark):
    num_buckets = 16
    
    spark.sql('drop table if exists matches_bucketed')
    # Create Maps and Matches DataFrames
    matches_df = spark.read.csv('../../data/matches.csv', header=True, inferSchema=True)

    # Repartition and Sort Within Partions
    matches_df = matches_df.repartition(num_buckets, 'match_id').sortWithinPartitions('match_id')
    matches_df.write.format('parquet').bucketBy(num_buckets, 'match_id').mode('overwrite').saveAsTable('matches_bucketed')
    
    # Read Bucketed Table
    matches_bucketed_df = spark.table('matches_bucketed')
    return matches_bucketed_df
    
def bucket_join_match_details(spark):
    num_buckets = 16
    spark.sql('drop table if exists match_details_bucketed')
    # Create Maps and Matches DataFrames
    match_details_df = spark.read.csv('../../data/match_details.csv', header=True, inferSchema=True)

    # Repartition and Sort Within Partions
    match_details_df = match_details_df.repartition(num_buckets, 'match_id').sortWithinPartitions('match_id')
    match_details_df.write.format('parquet').bucketBy(num_buckets, 'match_id').mode('overwrite').saveAsTable('match_details_bucketed')
    
    # Read Bucketed Table
    match_details_df_bucketed_df = spark.table('match_details_bucketed')
    return match_details_df_bucketed_df

def bucket_join_medals_matches_players(spark):
    num_buckets = 16
    
    # Create Maps and Matches DataFrames
    medals_matches_players_df = spark.read.csv('../../data/medals_matches_players.csv', header=True, inferSchema=True)

    # Repartition and Sort Within Partions
    spark.sql('drop table if exists medals_matches_players_bucketed')
    medals_matches_players_df = medals_matches_players_df.repartition(num_buckets, 'match_id').sortWithinPartitions('match_id')
    medals_matches_players_df.write.format('parquet').bucketBy(num_buckets, 'match_id').mode('overwrite').saveAsTable('medals_matches_players_bucketed')
    
    # Read Bucketed Table
    medals_matches_players_bucketed_df = spark.table('medals_matches_players_bucketed')
    medals_matches_players_bucketed_df.show()
    return medals_matches_players_bucketed_df

def bucket_join_everything(matches_df, match_details_df, medals_matches_players_df):
    # Bucketed all dataframes
    bucketed_df = matches_df.join(match_details_df, 'match_id').join(medals_matches_players_df, ['match_id', 'player_gametage'])
    return bucketed_df
    

def get_aggregate_stats(spark):
    # Get average kills per match per player
    pass

def main():
    # Create Spark Session
    spark = SparkSession.builder \
        .config('spark.sql.autoBroadcastJoinThreshold', -1) \
        .appName('MatchStats') \
        .getOrCreate()
        
    broadcast_map = broadcast_joib_maps(spark)
    broadcast_map.show()
    
    broadcast_medals = broadcast_join_medals(spark)
    broadcast_medals.show()
        
    bucketed_matches = bucket_join_matches(spark)
       
    bucketed_match_details = bucket_join_match_details(spark) 
    
    bucketed_medals_matches_players = bucket_join_medals_matches_players(spark)
    
    # Bucket join all DataFrames
    bucketed_df = bucket_join_everything(bucketed_matches, bucketed_match_details, bucketed_medals_matches_players)    
    bucketed_df.show()
    
if __name__ == '__main__':
    main()