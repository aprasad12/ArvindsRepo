#!/bin/bash
. $EDGE_ROOT/scripts/config.cfg

PROCESS_DATE_TIME=$(date +"%Y%m%d%H%M%S")
logfile=$LOG_ROOT/$PROCESS_DATE_TIME.log
echo -e "Starting Sqoop Pulls - log can be viewed at $logfile\n"

read -p "Enter Start Date as YYYYMMDD: " START_DT
START_DATE=$(date -d $START_DT +%Y%m%d)
if [[ $? != 0 ]]; then echo "Invalid start date, exiting."; exit 1; fi >> $logfile
read -p "Enter End Date as YYYYMMDD: " END_DT
END_DATE=$(date -d $END_DT +%Y%m%d)
if [[ $? != 0 ]]; then echo "Invalid end date, exiting."; exit 1; fi >> $logfile

START_TIME=$(date -d $START_DT +%s)
END_TIME=$(date -d $END_DT +%s)
if [ $START_TIME -gt $END_TIME ]; 
then 
    echo "Start date is greater than end date, exiting." >> $logfile
    exit 1
fi

START_DATES=()
END_DATES=()

CURRENT_DATE=$START_DATE
CURRENT_TIME=`date -d $CURRENT_DATE +%s`

START_DATES+=($START_DATE)
while [[ CURRENT_TIME -le END_TIME ]]
do
    CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 month" +%Y%m%d)
    CURRENT_TIME=$(date -d $CURRENT_DATE +%s)
    if [[ CURRENT_TIME -ge END_TIME ]]
    then
        END_DATES+=($END_DATE)
        break
    elif [[ CURRENT_TIME -lt END_TIME ]]
    then
        END_DATES+=($(date -d "$CURRENT_DATE - 1 day" +%Y%m%d))
        START_DATES+=($CURRENT_DATE)
    fi
done

echo -e "\nBeginning Sqoop with parameters:" >> $logfile
echo -e "-----------------------------------------------------" >> $logfile
echo -e "Teradata IP Address:                    "$TD_IP_ADDRESS >> $logfile
echo -e "Teradata Database Name:                 "$TD_DATABASE_NM >> $logfile
echo -e "Teradata Username:                      "$TD_USERNAME"\n" >> $logfile
echo -e "Sqoop home path:                        "$SQOOP_HOME >> $logfile
echo -e "Teradata Connector for Hadoop JAR path: "$TDCH_JAR >> $logfile
echo -e "Lib JARS path:                          "$LIB_JARS"\n" >> $logfile
echo -e "Writing Kantar AEF to:                  "$KANTAR_AEF_DIR >> $logfile
echo -e "Writing Kantar PATEF to:                "$KANTAR_PATEF_DIR >> $logfile
echo -e "Writing TWC PATEF to:                   "$TWC_PATEF_DIR >> $logfile
echo -e "Writing TWC AENF to:                    "$TWC_AENF_DIR"\n" >> $logfile
echo -e "Start Date:                             "$START_DATE >> $logfile
echo -e "End Date:                               "$END_DATE >> $logfile
echo -e "-----------------------------------------------------\n" >> $logfile

# echo "Setting up Hadoop Directories." >> $logfile
# hadoop fs -rm -r $HDFS_ROOT/data
# hadoop fs -rm -r $HDFS_ROOT/scored
# hadoop fs -rm -r $HDFS_ROOT/reports

# hadoop fs -mkdir $HDFS_ROOT/data
# hadoop fs -mkdir $HDFS_ROOT/scored
# hadoop fs -mkdir $HDFS_ROOT/reports

# echo -e "Beginning AM_ADVERTISER_NEW_DIM Sqoop." >> $logfile
# $SQOOP_IMPORT \
# --query "SELECT AD_CUST_KEY,KANTAR_AD_CUST_NM from AM_ADVERTISER_NEW_DIM" \
# --target-dir $AD_CUST_NM_LKUP_DIR \
# --num-mappers 1 \
# --fields-terminated-by "|"
# if [[ $? = 0 ]]; then echo -e "AM_ADVERTISER_NEW_DIM Sqoop Complete."; fi >> $logfile

# echo -e "Beginning AM_DAYPART_DIM Sqoop." >> $logfile
# $SQOOP_IMPORT \
# --query "SELECT DAYPART_KEY,DAYPART_NM from AM_DAYPART_DIM" \
# --target-dir $DAYPART_NM_LKUP_DIR \
# --num-mappers 1 \
# --fields-terminated-by "|"
# if [[ $? = 0 ]]; then echo -e "AM_DAYPART_DIM Sqoop Complete."; fi >> $logfile

# echo -e "Beginning AM_STATION_TYPE_CUBE Sqoop." >> $logfile
# $SQOOP_IMPORT \
# --query "SELECT $STN_NORM_NM_LKUP_COLS from AM_STATION_TYPE_CUBE" \
# --target-dir $STN_NORM_NM_LKUP_DIR \
# --num-mappers 1 \
# --fields-terminated-by "|"
# if [[ $? = 0 ]]; then echo -e "AM_STATION_TYPE_CUBE Sqoop Complete."; fi >> $logfile

# echo "Beginning AM_AD_EVENT_NEW_FACT Sqoop" >> $logfile
# $SQOOP_IMPORT \
# --query "SELECT $TWC_AENF_COLS FROM AM_AD_EVENT_NEW_FACT WHERE AD_EVNT_START_DAY_KEY BETWEEN $START_DATE AND $END_DATE" \
# --target-dir $TWC_AENF_DIR \
# --num-mappers 2 \
# --split-by INVC_NBR_TXT \
# --fields-terminated-by "|"
# if [[ $? = 0 ]]; then echo "AM_AD_EVENT_NEW_FACT Sqoop Complete."; fi >> $logfile

# while read dma_mapping; do
#     echo "Clearing directory $KANTAR_AEF_DIR to trash." >> $logfile
#     hadoop fs -rm -r $KANTAR_AEF_DIR
#     echo "Clearing directory $KANTAR_PATEF_DIR to trash." >> $logfile
#     hadoop fs -rm -r $KANTAR_PATEF_DIR
#     echo "Clearing directory $TWC_PATEF_DIR to trash." >> $logfile
#     hadoop fs -rm -r $TWC_PATEF_DIR

#     # Create these directories ahead of time so respective stage tables can be moved into them.
#     hadoop fs -mkdir $KANTAR_PATEF_DIR
#     hadoop fs -mkdir $TWC_PATEF_DIR

#     IFS=':' read -ra DMA_LKUP <<< "$dma_mapping"
#     DMA_NAME=${DMA_LKUP[0]}
#     DMA_CD_KEY=${DMA_LKUP[1]}
#     MAS_DIV_CDS=${DMA_LKUP[2]}
#     IFS=',' read -ra MAS_DIV_CD_ARRAY <<< "$MAS_DIV_CDS"

    # echo "Sqooping data for "$DMA_NAME". with DMA_CD_KEY: "$DMA_CD_KEY >> $logfile
    # FINAL_OUTPUT_DIR=$HDFS_ROOT/reports/$DMA_NAME

    # echo "Beginning AM_KANTAR_PROGRAM_AD_TUNING_EVENT_FACT Sqoop" >> $logfile
    # KANTAR_SQOOP_LOOPS=${#START_DATES[@]}
    # for i in "${!START_DATES[@]}"
    # do
    #     CURRENT_START_DT=${START_DATES[$i]}
    #     CURRENT_END_DT=${END_DATES[$i]}
    #     KANTAR_PATEF_MONTH_STAGE_DIR=$KANTAR_PATEF_STAGE_DIR/$CURRENT_START_DT

    #     echo -e "KANTAR PATEF SQOOP ($((i+1))/$KANTAR_SQOOP_LOOPS) beginning \n" >> $logfile
    #     $SQOOP_IMPORT \
    #     --query "SELECT $KANTAR_PATEF_COLS FROM AM_KANTAR_PROGRAM_AD_TUNING_EVENT_FACT WHERE AD_EVNT_START_DAY_KEY BETWEEN $CURRENT_START_DT AND $CURRENT_END_DT AND DMA_CD_KEY=$DMA_CD_KEY" \
    #     --target-dir $KANTAR_PATEF_MONTH_STAGE_DIR \
    #     --split-by AD_EVNT_INSTNC_KEY \
    #     --num-mappers 1 \
    #     --fields-terminated-by "|"
    #     if [[ $? = 0 ]]; then echo -e "KANTAR PATEF SQOOP ($((i+1))/$KANTAR_SQOOP_LOOPS) Complete \n"; fi >> $logfile

    #     echo "Moving KANTAR PORTION $CURRENT_START_DT PATEF from stage to collection directory." >> $logfile
    #     files=$(hadoop fs -ls $KANTAR_PATEF_MONTH_STAGE_DIR | awk  '!/^d/ {print $8}')
    #     for f in $files; do hadoop fs -mv $f $f-$CURRENT_START_DT; done
    #     hadoop fs -mv $KANTAR_PATEF_MONTH_STAGE_DIR/* $KANTAR_PATEF_DIR/
    #     hadoop fs -rm -r $KANTAR_PATEF_MONTH_STAGE_DIR
    # done
    # if [[ $? = 0 ]]; then echo -e "AM_KANTAR_PROGRAM_AD_TUNING_EVENT_FACT Sqoop Complete."; fi >> $logfile

    # echo "Beginning AM_PROGRAM_AD_TUNING_EVENT_FACT Sqoop"
    # for MAS_DIV_KEY in "${MAS_DIV_CD_ARRAY[@]}"
    # do
    #     MAS_TWC_PATEF_STAGE_DIR=$TWC_PATEF_STAGE_DIR/$MAS_DIV_KEY
    #     echo $MAS_TWC_PATEF_STAGE_DIR
    #     $SQOOP_IMPORT \
    #     --query "SELECT $TWC_PATEF_COLS FROM AM_PROGRAM_AD_TUNING_EVENT_FACT WHERE AD_EVNT_START_DAY_KEY BETWEEN $START_DATE AND $END_DATE AND MAS_DIV_KEY=$MAS_DIV_KEY" \
    #     --target-dir $MAS_TWC_PATEF_STAGE_DIR \
    #     --num-mappers 2 \
    #     --split-by AD_EVNT_INSTNC_KEY \
    #     --fields-terminated-by "|"

    #     echo "Moving MAS_DIV_CD $MAS_DIV_KEY PATEF from stage to collection directory."
    #     files=$(hadoop fs -ls $MAS_TWC_PATEF_STAGE_DIR | awk  '!/^d/ {print $8}')
    #     for f in $files; do hadoop fs -mv $f $f-$MAS_DIV_KEY; done
    #     hadoop fs -mv $MAS_TWC_PATEF_STAGE_DIR/* $TWC_PATEF_DIR/
    #     hadoop fs -rm -r $MAS_TWC_PATEF_STAGE_DIR
    # done
    # if [[ $? = 0 ]]; then echo -e "AM_PROGRAM_AD_TUNING_EVENT_FACT Sqoop Complete."; fi >> $logfile

    # echo "Beginning AM_KANTAR_AD_EVENT_FACT Sqoop." >> $logfile
    # $SQOOP_IMPORT \
    # --query "SELECT AD_EVNT_INSTNC_KEY,SPOT_RATE FROM AM_KANTAR_AD_EVENT_FACT WHERE AD_EVNT_START_DAY_KEY BETWEEN $START_DATE AND $END_DATE AND DMA_CD_KEY=$DMA_CD_KEY" \
    # --target-dir $KANTAR_AEF_DIR \
    # --num-mappers 1 \
    # --fields-terminated-by "|"
    # if [[ $? = 0 ]]; then echo -e 'KANTAR AEF Sqoop Complete \n'; fi >> $logfile

    # echo -e "AD CUST NM SQOOP ($((i+1))/2) beginning \n" >> $logfile
    # $SQOOP_IMPORT \
    # --query "SELECT AD_CUST_KEY,KANTAR_AD_CUST_NM from AM_ADVERTISER_NEW_DIM" \
    # --target-dir $AD_CUST_NM_LKUP_DIR \
    # --num-mappers 1 \
    # --fields-terminated-by "|"
    # if [[ $? = 0 ]]; then echo -e "AD CUST NM SQOOP ($((i+1))/2) Complete \n"; fi >> $logfile

    # echo -e "DAYPART NM SQOOP ($((i+1))/2) beginning \n" >> $logfile
    # $SQOOP_IMPORT \
    # --query "SELECT DAYPART_KEY,DAYPART_NM from AM_DAYPART_DIM" \
    # --target-dir $DAYPART_NM_LKUP_DIR \
    # --num-mappers 1 \
    # --fields-terminated-by "|"
    # if [[ $? = 0 ]]; then echo -e "DAYPART NM SQOOP ($((i+1))/2) Complete \n"; fi >> $logfile

    # echo -e "STN NM SQOOP ($((i+1))/2) beginning \n" >> $logfile
    # $SQOOP_IMPORT \
    # --query "SELECT $STN_NORM_NM_LKUP_COLS from AM_STATION_TYPE_CUBE" \
    # --target-dir $STN_NORM_NM_LKUP_DIR \
    # --num-mappers 1 \
    # --fields-terminated-by "|"
    # if [[ $? = 0 ]]; then echo -e "STN NM SQOOP ($((i+1))/2) Complete \n"; fi >> $logfile

    echo "Begin Genetic Algorithm" >> $logfile
    spark-submit \
    --master yarn-cluster \
    --num-executors $NUM_EXECUTORS \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --driver-memory $DRIVER_MEMORY \
    --jars ../jars/datanucleus-api-jdo-3.2.6.jar,../jars/datanucleus-core-3.2.10.jar,../jars/datanucleus-rdbms-3.2.9.jar \
    --py-files ../app/constants.py,../app/generate_report.py,../app/genetic_algorithm.py,../app/score_patef.py \
    --conf "spark.shuffle.reduceLocality.enabled=true" \
    --conf "spark.memory.useLegacyMode=false" \
    ../app/main.py \
    $TWC_PATEF_DIR \
    $TWC_AENF_DIR \
    $KANTAR_PATEF_DIR \
    $KANTAR_AEF_DIR \
    $STN_NORM_NM_LKUP_DIR \
    $AD_CUST_NM_LKUP_DIR \
    $DAYPART_NM_LKUP_DIR \
    $SCORED_TWC_PATEF_DIR \
    $SCORED_KANTAR_PATEF_DIR \
    $FINAL_OUTPUT_DIR \
    $TWC_PATEF_COLS \
    $TWC_AENF_COLS \
    $KANTAR_PATEF_COLS \
    $KANTAR_AEF_COLS \
    $STN_NORM_NM_LKUP_COLS \
    $AD_CUST_NM_LKUP_COLS \
    $DAYPART_NM_LKUP_COLS \
    $NUM_EXECUTORS \
    $EXECUTOR_CORES
    echo "Genetic Algorithm Complete" >> $logfile

done < $DMA_MAS_MAP

hadoop fs -get $HDFS_ROOT/reports $EDGE_ROOT >> $logfile
zip -r reports.zip $EDGE_ROOT/reports/*

mail \
-s "Schedule Reallocator Reports" \
-a reports.zip \
$REPORT_RECIPIENT <<< \
"See attachment"
