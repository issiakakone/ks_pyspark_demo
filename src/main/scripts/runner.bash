#%--------------------------------------------%#
#% Filename: runner.bash                      %#
#% Description: Submit the spark job          %#
#% Author: Issiaka Kone                       %#
#% Date: 04/04/2018                           %#
#%--------------------------------------------%#

SPARK_MASTER='local[*]'
DEPLOY_MODE='client'

[ -z "$SPARK_HOME" ]    && export SPARK_HOME=/opt/spark
[ -z "$KS_SPARK_HOME" ] && export KS_SPARK_HOME=/home/issiaka/pyspark/target/pyspark-1.0.0
[ -z "$KS_SPARK_HOME" ] && export KS_SPARK_HOME=/home/issiaka/pyspark/target/pyspark-1.0.0

arg="$1"
SCRIPTS_DIR="$KS_SPARK_HOME/scripts"

function usage {
    echo ""
	echo ""
	echo "Usage:"
	echo "    $(basename $0) [-h | --help | config_file_path]"
	echo ""
	echo ""
	exit 0
}

if [ -n "$arg" ]; then
	if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
		usage
	else
		CONFIG_FILE="$arg"
	fi
else
	CONFIG_FILE="$KS_SPARK_HOME/config/config.json"
fi

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