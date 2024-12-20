# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

df_master = spark.sql("select * from hive_metastore.default.leadgen_data_upload_4_csv")
df_master = df_master.withColumn('Mail_Date',  f.to_date(f.col('Week'), "M/d/y")).drop(f.col('Week')).dropDuplicates()

## Updating Tactic names ##
df_master = df_master.withColumn("New Tactic",  when(f.col('Tactic')=='CO-REG LEADS', 'CoRegLeads')\
                                               .when(f.col('Tactic')=='Major Rocket', 'MajorRocket')\
                                               .when(f.col('Tactic')=='OFFER WALLS', 'OfferWalls').otherwise(f.col('Tactic'))).drop('Tactic').withColumnRenamed("New Tactic","Tactic")

df_master = df_master.withColumn("New Spend", f.col('Spend')+f.col('Joins+Renews')*5).drop('Spend').withColumnRenamed('New Spend', 'Spend')
df_master = df_master.withColumn("Joins_Renews", f.col("Joins+Renews")).drop("Joins+Renews")

cols = ['Mail_Date', 'Tactic', 'Spend', 'Medium', 'Medium_Sent', 'Unique_Visitors', 'Joins_Renews']
df = df_master.select(cols).dropDuplicates()
df = df.filter(f.year(f.col('Mail_Date')).isin(['2020', '2021', '2022']))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FY 2023 Data Apend

# COMMAND ----------

df_2023 = spark.sql("select * from hive_metastore.default.leadgen_data_upload_h2_2023_csv")
df_2023 = df_2023.withColumn('Mail_Date',  f.to_date(f.col('Week'), "M/d/y")).drop(f.col('Week')).dropDuplicates()

## Updating Tactic names ##
df_2023 = df_2023.withColumn("New Tactic",  when(f.col('Tactic')=='CO-REG LEADS', 'CoRegLeads')\
                                               .when(f.col('Tactic')=='Major Rocket', 'MajorRocket')\
                                               .when(f.col('Tactic')=='OFFER WALLS', 'OfferWalls').otherwise(f.col('Tactic'))).drop('Tactic').withColumnRenamed("New Tactic","Tactic")

df_2023 = df_2023.withColumn("New Spend", f.col('Spend')+f.col('Joins+Renews')*5).drop('Spend').withColumnRenamed('New Spend', 'Spend')
df_2023 = df_2023.withColumn("Joins_Renews", f.col("Joins+Renews")).drop("Joins+Renews")

## Renaming Columns ##
df_2023 = df_2023.withColumnRenamed('Medium Sent','Medium_Sent')
df_2023 = df_2023.withColumnRenamed('Unique Visitors', 'Unique_Visitors')

cols = ['Mail_Date', 'Tactic', 'Spend', 'Medium', 'Medium_Sent', 'Unique_Visitors', 'Joins_Renews']
df_2023 = df_2023.select(cols).dropDuplicates()

## Filter dates for H1 2023##
df_2023 = df_2023.filter(f.year(f.col('Mail_Date'))==2023)
df_2023.display()

# COMMAND ----------

## Apepnding Data Sources ##
df = df.union(df_2023)


## Changing Column Names and adding DMA_Code Column ##
df = df.withColumnRenamed('Mail_Date', 'Date')
df = df.withColumnRenamed('Medium_Sent', 'Impression')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

# Numbers provided as a string
numbers_string = """
500
501
502
503
504
505
506
507
508
509
510
511
512
513
514
515
516
517
518
519
520
521
522
523
524
525
526
527
528
529
530
531
532
533
534
535
536
537
538
539
540
541
542
543
544
545
546
547
548
549
550
551
552
553
554
555
556
557
558
559
560
561
563
564
565
566
567
569
570
571
573
574
575
576
577
581
582
583
584
588
592
596
597
598
600
602
603
604
605
606
609
610
611
612
613
616
617
618
619
622
623
624
625
626
627
628
630
631
632
633
634
635
636
637
638
639
640
641
642
643
644
647
648
649
650
651
652
656
657
658
659
661
662
669
670
671
673
675
676
678
679
682
686
687
691
692
693
698
702
705
709
710
711
716
717
718
722
724
725
734
736
737
740
743
744
745
746
747
749
751
752
753
754
755
756
757
758
759
760
762
764
765
766
767
770
771
773
789
790
798
800
801
802
803
804
807
810
811
813
819
820
821
825
828
839
855
862
866
868
881
"""

# Convert numbers to integers and create a Python list
dma_code = [int(num) for num in numbers_string.strip().split("\n")]

# Print the Python list
print(dma_code)


# COMMAND ----------


final_model_df = df.select('Date', 'Tactic', 'Spend', 'Impression')

## Dividing Metrics by 210 to distribute data at national level ##
final_model_df = final_model_df.withColumn("Spend",f.col('Spend')/210)
final_model_df = final_model_df.withColumn("Impression",f.col('Impression')/210)

## Creating DMA DF ##
df_dma = spark.createDataFrame(dma_code, StringType()).toDF('DMA_Code')


## Cross Joining Data ##
final_model_df = final_model_df.crossJoin(df_dma)

## Renaimg Cols ##
final_model_df = final_model_df.withColumnRenamed('Tactic', 'SubChannel')
final_model_df = final_model_df.withColumnRenamed('Impression', 'Imps')
final_model_df = final_model_df.withColumnRenamed('DMA_Code', 'DmaCode')

## Adding New Cols ##
final_model_df = final_model_df.withColumn('Campaign', f.col('SubChannel'))
final_model_df = final_model_df.withColumn('Channel', when(f.col('SubChannel').isin(['Email']) ,'Email').otherwise('LeadGen') )
final_model_df = final_model_df.withColumn('MediaName', f.lit('LeadGen'))
final_model_df = final_model_df.withColumn('Clicks', f.lit(0))

## Re-arranging COlumns ##
cols = ['Date', 'DmaCode', 'MediaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols)

final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_LeadGen_LeadGen" 
save_df_func(final_model_df, table_name)

# COMMAND ----------

## Reading Data for Reporting ##
table_name = "temp.ja_blend_mmm2_LeadGen_LeadGen" 
df = load_saved_table(table_name)
# df.groupBy('Date', 'Channel', 'SubChannel', 'Campaign').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') ).display()
df.select('Channel', 'SubChannel', 'Campaign').dropDuplicates().display()

# COMMAND ----------


