# copies [~/.ssh/aws-hadoop.pem ~/.ssh/config]
# to [hnamenode hdatanode1 hdatanode2 hdatanode3]
# to directory '~/.ssh' at the destination

$SPARK_PROJECT_DIR/scripts/scp.py \
    '~/.ssh' \
    ~/.ssh/aws-hadoop.pem ~/.ssh/config -- \
    hnamenode hdatanode1 hdatanode2 hdatanode3
