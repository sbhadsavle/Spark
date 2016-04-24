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

There are four steps to get a project running:

1. Create instances
2. Setup SSH and communication
3. Setup Hadoop
4. Run programs, analyze and record performance

#### 1. Create instances

Create 4 instances using Amazon AWS or Digital Ocean. Create an `addresses.yaml` 
file with the relevant information using information from the AWS or Digital
Ocean websites. See `examples/addresses.yaml` for the structure of this file.


#### 2. SSH

At a local machine, generate and save an SSH config to `~/.ssh/config` for 
ease of use: 

```
$ scripts/sshconfig.py addresses.yaml ubuntu '~/.ssh/aws-hadoop.pem'
```

To setup inter-node communication, copy over the local `~/.ssh/config` and
`.pem` to the namenode and datanodes:

```
$ scripts/scp-ssh-config.sh
```

#### 3. Setup Hadoop

Copy over the Hadoop setup scripts to the namenode and datanodes:

```
$ hadoop/scp-setup-script.sh
```

SSH into the namenode and datanodes in separate terminals:

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

#### 4. Running


## Spark
