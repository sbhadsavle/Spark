# Analyzing Spark and Hadoop performance

## Directory structure

```
README.md : the file you are reading
examples/ : samples for config files, usage examples, etc.
hadoop/   : hadoop-specific setup scripts, code
spark/    : spark-specific setup scripts, code
scripts/  : general scripts applicable to both spark and hadoop, local scripts
```

## Hadoop

Create 4 instances using Amazon AWS or Digital Ocean. Create an `addresses.yaml` 
file with the relevant information (see examples for the structure) using
information from the AWS or Digital Ocean websites. 

#### SSH

At a local machine, generate and save a ssh config to `~/.ssh/config` for ease of use: 

```
$ scripts/sshconfig.py addresses.yaml ubuntu '~/.ssh/aws-hadoop.pem'
```

To setup inter-node communication, copy over the local `~/.ssh/config` and
key to the namenode and datanodes:

```
$ scripts/scp-ssh-config.sh
```

#### SETUP

Copy over the Hadoop setup scripts to the namenode and datanodes:

```
$ hadoop/scp-setup-script.sh
```

ssh into the namenode and datanodes:

```
$ ssh hnamenode
$ ssh hdatanode1
...
```

Run the setup scripts at the namenode and each of the datanodes:

```
$ chmod +x setup.py
$ sudo ./setup.py namenode addresses.yaml
```

```
$ chmod +x setup.py
$ sudo ./setup.py datanode addresses.yaml
```

#### RUNNING


## Spark
