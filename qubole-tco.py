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

import boto3
import pytz
from botocore.exceptions import ClientError


# Follow these https://github.com/qubole/tco/blob/master/README.md instructions before running this script
# access and secrete keys must be given as an input for eg:-
# python cloudwatch_automate.py --access-key=<accesskey> --secret-key=<secret_key>

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
        logger.info("Fetching cluster id's of last 60 days clusters, across all AWS regions.......")
        for region in aws_regions:
            client = boto3.client('emr', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=region)
            response = client.list_clusters(CreatedAfter=(now - timedelta(days=60)),
                                            CreatedBefore=datetime(now.year, now.month, now.day),
                                            ClusterStates=['TERMINATED', 'TERMINATING', 'WAITING', 'RUNNING'])
            sleep(2)
            logger.debug("clusters" % response)

            for i in range(0, len(response['Clusters'])):
                id = response['Clusters'][i]['Id']

                cluster_state = response['Clusters'][i]['Status']['State']
                print id
                if cluster_state == 'WAITING' or cluster_state == 'RUNNING' or cluster_state == 'TERMINATING':
                    print id
                    print cluster_state
                    end = now_e
                else:
                    print id
                    print cluster_state
                    end = response['Clusters'][i]['Status']['Timeline']['EndDateTime']

                time_stamp = end - response['Clusters'][i]['Status']['Timeline']['CreationDateTime']

                time_s = day_pattern.search(str(time_stamp))

                if time_s is not None:
                    cluster_id_timestamp.append({'cluster_id': id, 'time_stamp': time_s.group(1), 'region': region})

        logger.info("Fetched Clusters - %s" % json.dumps(cluster_id_timestamp))
    except ClientError as e:
        logger.error(e)
        sys.exit(0)

    logger.info("Selecting at most 25 longest running clusters........")
    cluster_id_timestamp = sorted(cluster_id_timestamp, key=itemgetter('time_stamp'), reverse=True)

    if cluster_id_timestamp is None:
        logger.error("No cluster found in aws emr account corresponding to given access tokens")

    for i in cluster_id_timestamp:
        if count >= 25:
            break
        cluster_id.append(i['cluster_id'])
        cluster_id_region.append({'cluster_id': i['cluster_id'], 'region': i['region']})
        count = count + 1

    if len(cluster_id_region) == 0:
        logger.error("You don't have any large cluster i.e cluster of atleast 10 nodes")
        # print "You don't have any large cluster i.e cluster of atleast 10 nodes"
        sys.exit(0)

    else:
        clusterid = ''.join(cluster_id)
        logger.info(
            "Shortlisted clusters on the basis of number of time stamp and cluster's timestamp are - \n %s" % clusterid)
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
        s_t = str(cluster_status['Cluster']["Status"]["Timeline"]["CreationDateTime"])
        s_t = s_t.split('.')[0]
        s_time = datetime.strptime(s_t, '%Y-%m-%d %H:%M:%S')
        mssg1 = "cluster_status['Cluster']['Status']['Timeline']['CreationDateTime'] = %s", \
                cluster_status['Cluster']['Status']['Timeline']['CreationDateTime']
        logger.debug(mssg1)

        if cluster_status['Cluster']["Status"]["State"] == "RUNNING" or cluster_status['Cluster']["Status"][
            "State"] == "WAITING" or cluster_status['Cluster']["Status"]["State"] == "STARTING" or \
                        cluster_status['Cluster']["Status"]["State"] == "TERMINATING":
            e_time = 1
        else:
            e_t = str(cluster_status['Cluster']["Status"]["Timeline"]["EndDateTime"])
            e_t = e_t.split('.')[0]
            e_time = datetime.strptime(e_t, '%Y-%m-%d %H:%M:%S')

            mssg2 = "cluster_status['Cluster']['Status']['Timeline']['EndDateTime'] = ", \
                    cluster_status['Cluster']['Status']['Timeline']['EndDateTime']
            logger.debug(mssg2)

        cluster_id_region_time.append(
            {'cluster_id': id['cluster_id'], 'region': id['region'], 's_time': s_time, 'e_time': e_time})

        try:

            cluster_applications = client.describe_cluster(ClusterId=id['cluster_id'])

        except ClientError as e:
            print e
            logger.error(e)
        cwd = os.getcwd() + "/emr_metrics"
        dir = cwd + "/" + id['cluster_id']
        del cluster_applications["Cluster"]["Status"]["Timeline"]
        content = json.dumps(cluster_applications, indent=4, sort_keys=True)
        if not os.path.exists(dir):
            os.makedirs(dir)
        with open(dir+"/describe_cluster.json", "w+") as f:
            f.write(content)

        if cluster_applications['Cluster']['InstanceCollectionType'] == "INSTANCE_GROUP":
            try:
                cluster_status = client.list_instance_groups(ClusterId=id['cluster_id'])
            except ClientError as e:
                print e
                logger.error(e)

            cwd = os.getcwd() + "/emr_metrics"
            dir = cwd + "/" + id['cluster_id']
            for i in range(0, len(cluster_status['InstanceGroups'])):
                del cluster_status['InstanceGroups'][i]['Status']['Timeline']
            content = json.dumps(cluster_status, indent=4, sort_keys=True)
            if not os.path.exists(dir):
                os.makedirs(dir)
            with open(dir+"/instance_group.json", "w+") as f:
                f.write(content)

        else:
            try:
                cluster_status = client.list_instance_fleets(ClusterId=id['cluster_id'])

            except ClientError as e:
                print e
                logger.error(e)
            for i in range(0, len(cluster_status['InstanceFleets'])):
                del cluster_status['InstanceFleets'][i]['Status']['Timeline']
            cwd = os.getcwd() + "/emr_metrics"
            dir = cwd + "/" + id['cluster_id']
            content = json.dumps(cluster_status, indent=4, sort_keys=True)
            if not os.path.exists(dir):
                os.makedirs(dir)
            with open(dir+"/instance_fleet.json", "w+") as f:
                f.write(content)

    logger.info("All Cluster details are fetched!")
    # logger.debug(cluster_node_details)
    return access_key, secret_key, cluster_id_region_time


def cloudwatch_metric():
    top_dir = "emr_metrics"
    top_cwd = os.getcwd() + "/" + top_dir
    if not os.path.exists(top_cwd):
        os.makedirs(top_cwd)
    access_key, secret_key, cluster_id_region_time = cluster_details()

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

    for i in cluster_id_region_time:
        try:
            client = boto3.client('cloudwatch', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=i['region'])
        except ClientError:
            logger.error("Please check your secret key and access key")
            sys.exit(0)

        # cwd = os.getcwd() + "/" + top_dir
        # dir = cwd + "/" + i['cluster_id']
        # if not os.path.exists(dir):
        #     os.makedirs(dir)
        print i
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
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryAvailableMB Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)
            # response
            # response = json.dumps(response, default=datetime_handler)
            # cwd = os.getcwd() + "/emr_metrics"
            # dir = cwd + "/" + i['cluster_id']
            file_name = "emr_metrics/" + i['cluster_id'] + "/" + "MemoryAvailableMB_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w+') as f:
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
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryTotalMB Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = "emr_metrics/" + i['cluster_id'] + "/" + "MemoryTotalMB_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w+') as f:
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
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("TaskNodesRunning Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = "emr_metrics/" + i['cluster_id'] + "/" + "TaskNodesRunning_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w+') as f:
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
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("CoreNodesRunning Metrics obtained for cluster %s " % i['cluster_id'])
            logger.debug(response)

            file_name = "emr_metrics/" + i['cluster_id'] + "/" + "CoreNodesRunning_%s.ans" % (i['cluster_id'])
            if not os.path.exists(file_name):
                with open(file_name, 'w+') as f:
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
    return {"message": "files created successfully in the same location", "status": "success"}


if __name__ == "__main__":
    cloudwatch_metric()
