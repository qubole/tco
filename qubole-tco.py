import argparse
import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timedelta
from operator import itemgetter
from time import sleep, gmtime, mktime, strptime, strftime
from boto.s3.connection import S3Connection
import boto3
import pytz
from botocore.exceptions import ClientError


# Follow these https://github.com/qubole/tco/blob/master/README.md instructions before running this script
# access and secrete keys must be given as an input for eg:-
# python cloudwatch_automate.py --access_key=<accesskey> --secret_key=<secret_key>

def cluster_details():
    logger = logging.getLogger('CloudwatchLog')
    logger.setLevel(logging.INFO)
    filelog = logging.FileHandler('Cloudwatch.log')
    filelog.setLevel(logging.DEBUG)
    consolelog = logging.StreamHandler()
    consolelog.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    filelog.setFormatter(formatter)
    consolelog.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(consolelog)
    logger.addHandler(filelog)

    now = datetime.now()
    cluster_id = []
    cluster_id_region = []
    cluster_id_timestamp = []
    cluster_id_region_time = []
    parser = argparse.ArgumentParser(description='Program to fetch cluster details of customer')
    parser.add_argument('--access-key', action="store", dest="access_key", help='Enter your aws access key',
                        required=True)
    parser.add_argument('--secret-key', action="store", dest="secret_key", help='Enter your aws secret key',
                        required=True)

    results = parser.parse_args()
    access_key = results.access_key
    secret_key = results.secret_key
    cluster_node_details = {'clusters': []}
    master_instance_type = None
    worker_instance_type = None
    task_instance_type = None
    master_market = None
    task_market = None
    worker_market = None
    count = 0

    aws_regions = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2', 'ap-south-1', 'ap-northeast-2', 'ap-southeast-1',
                   'ap-southeast-2', 'ap-northeast-1', 'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2',
                   'sa-east-1']
    utc = pytz.UTC
    now_e = datetime.now()
    now_e = utc.localize(now_e)
    # regex to extract day from date time of cluster timeLine
    day_pattern = re.compile("([\d]{1,4}) day")

    try:
        logger.info("Fetching cluster id's across all AWS regions.......")
        for region in aws_regions:
            client = boto3.client('emr', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            response = client.list_clusters(CreatedAfter=(now - timedelta(days=60)),
                                            ClusterStates=['TERMINATED', 'TERMINATING', 'WAITING', 'RUNNING'])
            sleep(2)
            logger.debug("clusters" % response)
            for i in range(0, len(response['Clusters'])):
                id = response['Clusters'][i]['Id']

                cluster_state = response['Clusters'][i]['Status']['State']
                if cluster_state == 'WAITING' or cluster_state == 'RUNNING' or cluster_state == 'TERMINATING':
                    end = now_e
                else:
                    end = response['Clusters'][i]['Status']['Timeline']['EndDateTime']

                time_stamp = end - response['Clusters'][i]['Status']['Timeline']['CreationDateTime']

                # time_s = day_pattern.search(str(time_stamp))
                time_s = str(time_stamp).split(' ')[0]
                if time_s is not None:
                    cluster_id_timestamp.append({'cluster_id': id, 'time_stamp': time_s, 'region': region})

        logger.info("Fetched Clusters - %s" % json.dumps(cluster_id_timestamp))
    except ClientError as e:
        logger.error(e)
        sys.exit(0)

    logger.info("Selecting at most 25 longest running clusters........")
    cluster_id_timestamp = sorted(cluster_id_timestamp, key=itemgetter('time_stamp'), reverse=True)

    if cluster_id_timestamp is None:
        logger.error("No cluster found in aws emr account corresponding to given access tokens")

    # change the logic of ordering the cluster i.e order on the basis of largest size cluster and timestamp must be
    # atleast 10 days

    for i in cluster_id_timestamp:
        if count >= 25:
            break
        cluster_id.append(i['cluster_id'])
        cluster_id_region.append({'cluster_id': i['cluster_id'], 'region': i['region']})
        count = count + 1
        # print info message to output the shortlisted clusters
    if len(cluster_id_region) == 0:
        logger.error("You don't have any large cluster i.e cluster of atleast 10 nodes")
        # print "You don't have any large cluster i.e cluster of atleast 10 nodes"
        sys.exit(0)

    else:
        clusterid = ''.join(cluster_id)
        logger.info("Shortlisted clusters on the basis of number of time stamp and cluster's timestamp are - \n %s" % clusterid)
        # print "Shortlisted clusters on the basis of number of time stamp and cluster's timestamp are %s" % cluster_id

    logger.info("Fetching cluster details of shortlisted clusters, this will take some time"
                ".....")
    for id in cluster_id_region:
        try:
            client = boto3.client('emr', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=id['region'])
            stdout = client.describe_cluster(
                ClusterId=id['cluster_id']
            )

        except ClientError as e:
            print e
            logger.error(e)
            sys.exit(0)

        cluster_status = stdout
        # print "s_time", cluster_status['Cluster']["Status"]["Timeline"]["CreationDateTime"]
        s_t = str(cluster_status['Cluster']["Status"]["Timeline"]["CreationDateTime"])
        # if '+' in s_t:
        #     s_t = s_t.split('+')[0]
        # elif '-' in s_t:
        #     s_t = s_t.split('-')[0]
        # else:
        #     zone_diff = ""
        s_t = s_t.split('.')[0]
        # date_format = "%Y-%m-%d %H:%M:%S"+s_t
        # s_t = strftime("%Y-%m-%d %H:%M:%S", gmtime(mktime(strptime(s_t, "%Y-%m-%d %H:%M:%S"))))
        s_time = datetime.strptime(s_t, '%Y-%m-%d %H:%M:%S')
        mssg1 = "cluster_status['Cluster']['Status']['Timeline']['CreationDateTime'] = %s", \
                cluster_status['Cluster']['Status']['Timeline']['CreationDateTime']
        logger.debug(mssg1)

        if cluster_status['Cluster']["Status"]["State"] == "RUNNING" or cluster_status['Cluster']["Status"][
            "State"] == "WAITING" or cluster_status['Cluster']["Status"]["State"] == "STARTING" or cluster_status['Cluster']["Status"]["State"] == "TERMINATING":
            e_time = 1
        else:
            e_t = str(cluster_status['Cluster']["Status"]["Timeline"]["EndDateTime"])
            e_t = e_t.split('.')[0]
            # date_format = "%Y-%m-%d %H:%M:%S" + e_t
            # e_t = strftime("%Y-%m-%d %H:%M:%S", gmtime(mktime(strptime(e_t, "%Y-%m-%d %H:%M:%S"))))
            e_time = datetime.strptime(e_t, '%Y-%m-%d %H:%M:%S')

            mssg2 = "cluster_status['Cluster']['Status']['Timeline']['EndDateTime'] = ", \
                    cluster_status['Cluster']['Status']['Timeline']['EndDateTime']
            logger.debug(mssg2)

        cluster_id_region_time.append({'cluster_id': id['cluster_id'], 'region': id['region'], 's_time': s_time, 'e_time': e_time})

        try:
            cluster_status = client.list_instance_groups(
                ClusterId=id['cluster_id']
            )

            cluster_applications = client.describe_cluster(
                ClusterId=id['cluster_id']
            )
        except ClientError as e:
            print e
            logger.error(e)

        for i in range(0, len(cluster_status['InstanceGroups'])):
            if cluster_status['InstanceGroups'][i]['InstanceGroupType'] == 'MASTER':
                master_instance_type = cluster_status['InstanceGroups'][i]['InstanceType']
                master_market = cluster_status['InstanceGroups'][i]['Market']
            elif cluster_status['InstanceGroups'][i]['InstanceGroupType'] == 'CORE':
                worker_instance_type = cluster_status['InstanceGroups'][i]['InstanceType']
                worker_market = cluster_status['InstanceGroups'][i]['Market']
            else:
                task_instance_type = cluster_status['InstanceGroups'][i]['InstanceType']
                task_market = cluster_status['InstanceGroups'][i]['Market']

        cluster_node_details.get('clusters').append(
            {
                "cluster_id": id['cluster_id'],
                "master_instance_type": master_instance_type,
                "master_market_buy": master_market,
                "worker_instance_type": worker_instance_type,
                "worker_market_buy": worker_market,
                "task_instance_type": task_instance_type,
                "task_market": task_market,
                "applications": cluster_applications['Cluster']['Applications']
            })
    logger.info("All Cluster details are fetched!")
    logger.debug(cluster_node_details)
    return access_key, secret_key, cluster_node_details, cluster_id_region_time


def cloudwatch_metric():
    access_key, secret_key, cluster_node_details, cluster_id_region_time = cluster_details()
    logger = logging.getLogger('CloudwatchLog')
    logger.setLevel(logging.INFO)
    filelog = logging.FileHandler('Cloudwatch.log')
    filelog.setLevel(logging.DEBUG)
    consolelog = logging.StreamHandler()
    consolelog.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    filelog.setFormatter(formatter)
    consolelog.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(consolelog)
    logger.addHandler(filelog)
    logger.info("Now fetching cloudwatch metrics of selected 10 clusters....")

    top_dir = "emr_metrics"
    top_cwd = os.getcwd() + "/" + top_dir
    if not os.path.exists(top_cwd):
        os.makedirs(top_cwd)
    content = json.dumps(cluster_node_details, indent=4, sort_keys=True)
    file = top_cwd + "/cluster_config.json"
    with open(file, 'w') as f:
        f.write(content)

    if not os.path.exists(top_dir):
        os.makedirs(top_dir)

    for i in cluster_id_region_time:
        try:
            client = boto3.client('cloudwatch', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=i['region'])
        except ClientError:
            logger.error("Please check your secret key and access key")
            sys.exit(0)

        cwd = os.getcwd() + "/" + top_dir
        dir = cwd + "/" + i['cluster_id']
        if not os.path.exists(dir):
            os.makedirs(dir)

        endTime = None
        startTime = i['s_time']
        days_left = sys.maxint
        while days_left > 0:
            endTime = startTime + timedelta(days=5)
            logger.info("Fetching MemoryAvailableMB metric for cluster with id %s", i['cluster_id'])
            logger.debug("Start time of cluster %s" % startTime)
            logger.debug("End time of cluster %s" % endTime)
            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='MemoryAvailableMB',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': i['cluster_id']
                                                        },
                                                    ],
                                                    StartTime=str(startTime).split(" ")[0],
                                                    EndTime=str(endTime).split(" ")[0],
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryAvailableMB Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)
            #response = json.dumps(response, default=datetime_handler)
            file_name = dir + "/" + "MemoryAvailableMB_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            logger.info("Fetching MemoryTotalMB metric for cluster with id %s", i['cluster_id'])
            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='MemoryTotalMB',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': i['cluster_id']
                                                        },
                                                    ],
                                                    StartTime=str(startTime).split(" ")[0],
                                                    EndTime=str(endTime).split(" ")[0],
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryTotalMB Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = dir + "/" + "MemoryTotalMB_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            logger.info("Fetching TaskNodesRunning metric for cluster with id %s", i['cluster_id'])
            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='TaskNodesRunning',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': i['cluster_id']
                                                        },
                                                    ],
                                                    StartTime=str(startTime).split(" ")[0],
                                                    EndTime=str(endTime).split(" ")[0],
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("TaskNodesRunning Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = dir + "/" + "TaskNodesRunning_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            logger.info("Fetching CoreNodesRunning metric for cluster with id %s", i['cluster_id'])

            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='CoreNodesRunning',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': i['cluster_id']
                                                        },
                                                    ],
                                                    StartTime=str(startTime).split(" ")[0],
                                                    EndTime=str(endTime).split(" ")[0],
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("CoreNodesRunning Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = dir + "/" + "CoreNodesRunning_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            if i['e_time'] == 1:
                diff = datetime.now() - startTime
            else:
                diff = i['e_time'] - startTime
            days_left = min(5, diff.days)
            logger.debug("Days left = %s" % days_left)
            startTime = startTime + timedelta(days=days_left)

    now = datetime.now().strftime('%s')
    zipfile = 'emr_metrics_%s' % now
    shutil.make_archive(zipfile, 'zip', './emr_metrics')
    shutil.rmtree('./emr_metrics')

    logger.info("Success!  file %s.zip is created successfully in the same location." % zipfile)
    s3 = boto3.resource('s3')
    zipfile = zipfile+".zip"
    s3.Bucket('emr-tco').upload_file(zipfile, zipfile)
    return {"message": "files created successfully in the same location", "status": "success"}


if __name__ == "__main__":
    cloudwatch_metric()
