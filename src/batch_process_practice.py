from pyspark.sql import SparkSession, functions as func, DataFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def top_5_performing_partners(df: DataFrame) -> DataFrame: # VCR = Video views/ Video Completion, VTR = Video views/ Impressions
    '''Top 5 performing partners by VCR and VTR.
    return df with cols:
        site, VCR, VTR
    '''
    return (df
            .filter(
                (func.col('video_completes') > 0) & 
                (func.col('impressions') > 0))
            .groupBy('site')
            .agg(
                (func.sum('video_views') / func.sum('video_completes')).alias('VCR'),
                (func.sum('video_views') / func.sum('impressions')).alias('VTR')
            )
            .orderBy(
                func.desc('VCR'),
                func.desc('VTR')
            )
            .limit(5))

def campaign_partner_device_metrics(df: DataFrame) -> DataFrame: # CPC / CTR, CPC = Total cost clicks / Total clicks
    '''CTR and CPC for each campaign-partner-device combination.
    return df with cols:
        campaign, site, device, CTR(%), CPC($), 
        total_impressions, total_clicks, total_spend
    '''
    return (df
              .filter(
                  (func.col('clicks') > 0) &
                  (func.col('impressions') > 0) &
                  (func.col('actualized_spend') > 0)
              )
              .groupBy('campaign', 'site', 'device')
              .agg(
                  (func.sum('clicks') / func.sum('impressions') * 100).alias('CTR(%)'),  # CTR (Click Through Rate) as percentage
                  (func.sum('actualized_spend') / func.sum('clicks')).alias('CPC($)'),  # CPC (Cost Per Click) - total spend divided by total clicks
                  func.sum('impressions').alias('total_impressions'),
                  func.sum('clicks').alias('total_clicks'),
                  func.sum('actualized_spend').alias('total_spend')
              )
              .orderBy(
                  func.desc('CTR(%)'),
                  func.asc('CPC($)')
              ))

def video_completion_rate(df: DataFrame) -> DataFrame:
    '''
    Video views that were not completed with %, VCR.
    return df with cols:
        campaign, site, total_video_completed, 
        total_video_views, completion_percent, 
        not_completed_percent
    '''
    return (df
              .filter(func.col('video_views') > 0)
              .groupBy('campaign', 'site')
              .agg(
                  func.sum('video_completes').alias('total_video_completed'),
                  func.sum('video_views').alias('total_video_views')
              ) 
              .withColumn(
                  'completion_percent',
                  (func.col('total_video_completed') / func.col('total_video_views')) * 100
              )
              .withColumn(
                  'not_completed_percent',
                  100 - func.col('completion_percent')
              )
              .orderBy(func.desc('not_completed_percent')))

def engagement_rate_by_channel(df: DataFrame) -> DataFrame:
    '''
    Compare engagement rate between video and non video channels.
    return df with cols:
        channel, engagement_rate
    '''
    return (df
            .filter(func.col('impressions') > 0)
            .groupBy('channel')
            .agg(
                (func.sum('engagements') / func.sum('impressions')).alias('engagement_rate')
            )
            .orderBy(func.desc('engagement_rate')))

def main():
    spark = SparkSession.builder.appName("Batch Process Practice").getOrCreate()
    df = spark.read.parquet("files/video_metrics.parquet") # .repartition('campaign')

    df.printSchema()

    top_partners_metrics = top_5_performing_partners(df)
    logger.info("Top 5 performing partners by VCR and VTR:")
    top_partners_metrics.show()

    campaign_metrics = campaign_partner_device_metrics(df)
    logger.info("CTR and CPC for each campaign-partner-device combination:")
    campaign_metrics.show()

    video_completion_metrics = video_completion_rate(df)
    logger.info("Video completion rate:")
    video_completion_metrics.show()

    engagement_metrics = engagement_rate_by_channel(df)
    logger.info("Engagement rate by channel:")    
    engagement_metrics.show()

    spark.stop()

if __name__ == "__main__":
    main()
