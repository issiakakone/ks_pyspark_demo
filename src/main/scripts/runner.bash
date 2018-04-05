#%--------------------------------------------%#
#% Filename: runner.bash                      %#
#% Description: Submit the spark job          %#
#% Author: Issiaka Kone                       %#
#% Date: 04/04/2018                           %#
#%--------------------------------------------%#

[ -z "$SPARK_HOME" ]    && export SPARK_HOME=/opt/spark
[ -z "$KS_SPARK_HOME" ] && export KS_SPARK_HOME=/home/issiaka/pyspark/target/pyspark-1.0.0

SCRIPTS_DIR="$KS_SPARK_HOME/scripts"
CONFIG_FILE="$KS_SPARK_HOME/config/config.json"

SPARK_MASTER='local[*]'
DEPLOY_MODE='client'

echo ""
echo ""
echo "CONFIG_FILE=  '$CONFIG_FILE'"
echo "SPARK_HOME=   '$SPARK_HOME'"
echo "SPARK_MASTER= '$SPARK_MASTER'"
echo "DEPLOY_MODE=  '$DEPLOY_MODE'"

echo ""
echo ""
echo "Submiting data parser to Spark..."
echo "Job started at $(date)."

echo ""
echo ""
echo "$SPARK_HOME/bin/spark-submit --master "$SPARK_MASTER" --deploy-mode "$DEPLOY_MODE" --py-files $SCRIPTS_DIR/toolkit.py,$SCRIPTS_DIR/__init__.py --files $CONFIG_FILE $SCRIPTS_DIR/main.py $CONFIG_FILE"

$SPARK_HOME/bin/spark-submit --master "$SPARK_MASTER" --deploy-mode "$DEPLOY_MODE" --py-files $SCRIPTS_DIR/toolkit.py,$SCRIPTS_DIR/__init__.py --files $CONFIG_FILE $SCRIPTS_DIR/main.py $CONFIG_FILE

echo ""
echo ""
echo "Job ended at $(date)."