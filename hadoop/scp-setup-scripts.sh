# copies [addresses.yaml hadoop/setup.py]
# to [hnamenode hdatanode1 hdatanode2 hdatanode3]
# to directory '~/' at the destination

$SPARK_PROJECT_DIR/scripts/scp.py \
    '~/' \
    $SPARK_PROJECT_DIR/addresses.yaml $SPARK_PROJECT_DIR/hadoop/setup.py -- \
    hnamenode hdatanode1 hdatanode2 hdatanode3
