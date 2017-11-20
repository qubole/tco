import json
import os
import re
import sys
from time import strftime, gmtime
import boto3
import argparse
import shutil
import pytz
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from operator import itemgetter
import logging
from bson import json_util
from dateutil.tz import tzlocal


# Before running this script, download and install aws cloud_watch cli using this doc -
# http://docs.aws.amazon.com/AmazonCloudWatch/latest/cli/SetupCLI.html
# you don't need to set path of credential file path if you are passing it to this script from command line argument


# list of cluster id's and email id of customer  must be given through command line argument
# eg :- python cloudwatch_automate.py --access_key=<accesskey> --secret_key=<secret_key> --client=qubole  --email=h@qubole.com

def cluster_details():
    logger = logging.getLogger('CloudwatchLog')
    logger.setLevel(logging.DEBUG)
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

    start_time = []
    end_time = []
    now = datetime.now()
    cluster_id = []
    cluster_id_region = []
    cluster_id_timestamp = []
    # db = MySQLdb.connect("localhost", "root", "dell", "emrtcodb")
    # cursor = db.cursor()
    parser = argparse.ArgumentParser(description='Program to fetch cluster details of customer')
    parser.add_argument('--access-key', action="store", dest="access_key", help='Enter your aws access key',
                        required=True)
    parser.add_argument('--secret-key', action="store", dest="secret_key", help='Enter your aws secret key',
                        required=True)
    parser.add_argument('--client', action="store", dest="user_client", default='user',
                        help='Enter --client=qubole if qubole is '
                             'running it or --client=User if customer '
                             'is running this script', required=True)

    parser.add_argument('--email', action="store", dest="email", help='Enter email id of customer', required=True)

    results = parser.parse_args()
    user_client = results.user_client
    access_key = results.access_key
    secret_key = results.secret_key
    email = results.email
    logging.debug("Client = %s" % user_client)
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
    day_pattern = re.compile("([\d]{1,4}) day")

    try:
        for region in aws_regions:
            print "region=", region
            client = boto3.client('emr', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=region)
            response = client.list_clusters(CreatedAfter=(now - timedelta(days=90)),
                                            CreatedBefore=datetime(now.year, now.month, now.day),
                                            ClusterStates=['TERMINATED', 'TERMINATING', 'WAITING', 'RUNNING'])
            logger.debug("clusters" % response)
            print response
            for i in range(0, len(response['Clusters'])):
                id = response['Clusters'][i]['Id']
                cluster_status = client.list_instances(
                    ClusterId=id
                )
                print "region==", region
                logger.info("Fetched Cluster id %s in %s" % (id, region))
                cluster_state = response['Clusters'][i]['Status']['State']
                if cluster_state == 'WAITING' or cluster_state == 'RUNNING' or cluster_state == 'TERMINATING':
                    end = now_e
                else:
                    end = response['Clusters'][i]['Status']['Timeline']['EndDateTime']
                time_stamp = end - response['Clusters'][i]['Status']['Timeline']['CreationDateTime']

                time_s = day_pattern.search(str(time_stamp))

                if time_s != None:
                    cluster_id_timestamp.append({'cluster_id': id, 'time_stamp': time_s.group(1), 'region': region,
                                                 'total_nodes': len(cluster_status['Instances'])})

    except ClientError:
        logger.error("Please check your secret key and access key")
        sys.exit(0)

    logger.info("Selecting 10 longest running clusters........")
    cluster_id_timestamp = sorted(cluster_id_timestamp, key=itemgetter('total_nodes'), reverse=True)

    if cluster_id_timestamp is None:
        logger.error("No cluster found in aws emr account corresponding to given access tokens")
    # change the logic of ordering the cluster i.e order on the basis of largest size cluster and timestamp must be
    # atleast 10 days
    for i in cluster_id_timestamp:
        if count >= 10:
            break
        if int(i['time_stamp']) >= 10:
            print "time_stamp", time_stamp
            cluster_id.append(i['cluster_id'])
            cluster_id_region.append({'cluster_id': i['cluster_id'], 'region': i['region']})
            count = count + 1
        # print info message to output the shortlisted clusters
    if len(cluster_id) == 0:
        logger.error("You don't have any large cluster i.e cluster of atleast 10 nodes")
        print "You don't have any large cluster i.e cluster of atleast 10 nodes"
        sys.exit(0)

    else:
        logger.info(
            "Shortlisted clusters on the basis of number of time stamp and cluster's timestamp are %s" % cluster_id)
        print "Shortlisted clusters on the basis of number of time stamp and cluster's timestamp are %s" % cluster_id

    for id in cluster_id_region:
        try:
            client = boto3.client('emr', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                  region_name=id['region'])
            stdout = client.describe_cluster(
                ClusterId=id['cluster_id']
            )

        except ClientError:
            logger.error("Please check your secret key and access key")
            sys.exit(0)
            # print "cluster_id==>>", cluster_id
        cluster_status = stdout
        s_time = datetime.strptime(str(cluster_status['Cluster']["Status"]["Timeline"]["CreationDateTime"]), '%Y-%m-%d '
                                                                                                             '%H:%M'
                                                                                                             ':%S.%f'
                                                                                                             '+05:30')
        mssg1 = "cluster_status['Cluster']['Status']['Timeline']['CreationDateTime'] = %s", \
                cluster_status['Cluster']['Status']['Timeline']['CreationDateTime']
        logger.debug(mssg1)
        if cluster_status['Cluster']["Status"]["State"] == "RUNNING" or cluster_status['Cluster']["Status"][
            "State"] == "WAITING" or cluster_status['Cluster']["Status"]["State"] == "STARTING":
            end_time.append(1)
        else:
            e_time = datetime.strptime(str(cluster_status['Cluster']["Status"]["Timeline"]["EndDateTime"]), '%Y-%m-%d '
                                                                                                            '%H:%M:%S'
                                                                                                            '.%f+05:30')

            mssg2 = "cluster_status['Cluster']['Status']['Timeline']['EndDateTime'] = ", \
                    cluster_status['Cluster']['Status']['Timeline']['EndDateTime']
            logger.debug(mssg2)
            end_time.append(e_time)
        start_time.append(s_time)
        # also add applications in this list
        cluster_status = client.list_instance_groups(
            ClusterId=id['cluster_id']
        )

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
                'cluster_id': id['cluster_id'],
                'master_instance_type': master_instance_type,
                'master_market_buy': master_market,
                'worker_instance_type': worker_instance_type,
                'worker_market_buy': worker_market,
                'task_instance_type': task_instance_type,
                'task_market': task_market
            })
    # insert_query = """INSERT INTO ClusterDetails (CLUSTER_ID,  CLUSTER_REGION, master_instance_type,
    #                    core_instance_type, task_instance_type, MARKET_BUY) VALUES ("%s", "%s", "%s", "%s", "%s", "%s");""" % \
    #                    (id['cluster_id'], id['region'], master_instance_type, worker_instance_type, task_instance_type,
    #                     worker_market)
    #     logger.debug(insert_query)
    #     try:
    #         # Execute the SQL command
    #         cursor.execute(insert_query)
    #         # Commit your changes in the database
    #         db.commit()
    #     except:
    #         # Rollback in case there is any error
    #         db.rollback()
    #
    # # disconnect from server
    # db.close()
    logger.info("All Cluster details are fetched!")
    logger.info("Done")
    logger.debug(cluster_node_details)
    print "cluster id", cluster_id
    return start_time, end_time, cluster_id, access_key, secret_key, user_client, email, cluster_node_details


def cloudwatch_metric():
    start_time, end_time, cluster_id, access_key, secret_key, client, email, cluster_node_details = cluster_details()
    print "cluster_id =========", cluster_id
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
    logger.info("Now Fetching Cloudwatch metrics of selected 10 clusters....")
    logger.debug(start_time)
    logger.debug(end_time)
    logger.debug(start_time)

    logger.debug("client = %s" % client)
    top_dir = "emr_metrics"
    top_cwd = os.getcwd() + "/" + top_dir
    if not os.path.exists(top_cwd):
        os.makedirs(top_cwd)
    file = top_cwd + "/cluster_config.json"
    with open(file, 'w') as f:
        f.write(str(cluster_node_details))

    try:
        client = boto3.client('cloudwatch', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    except ClientError:
        logger.error("Please check your secret key and access key")
        sys.exit(0)

    if not os.path.exists(top_dir):
        os.makedirs(top_dir)
    logger.debug("length of start_time list = ", len(start_time))
    for i in range(len(cluster_id)):
        cwd = os.getcwd() + "/" + top_dir
        dir = cwd + "/" + cluster_id[i]
        if not os.path.exists(dir):
            os.makedirs(dir)

        endTime = None
        startTime = start_time[i]
        days_left = sys.maxint
        print "new_ starttime=", startTime
        while days_left > 0:
            endTime = startTime + timedelta(days=5)
            print "starttime=", startTime
            print "end_time=", endTime
            logger.info("Fetching MemoryAvailableMB metric for cluster with id %s", cluster_id[i])
            logger.debug("Start time of cluster %s" % startTime)
            logger.debug("End time of cluster %s" % endTime)
            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='MemoryAvailableMB',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': cluster_id[i]
                                                        },
                                                    ],
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryAvailableMB Metrcs obtained for cluster %s " % cluster_id[i])
            logger.debug(response)
            response = json.dumps(response, indent=4, default=json_util.default)
            print str(response)
            file_name = dir + "/" + "MemoryAvailableMB_%s.ans" % (cluster_id[i])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='MemoryTotalMB',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': cluster_id[i]
                                                        },
                                                    ],
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("MemoryTotalMB Metrcs obtained for cluster %s " % cluster_id[i])
            logger.debug(response)
            response = json.dumps(response, indent=4, default=json_util.default)
            print "respon=", response
            file_name = dir + "/" + "MemoryTotalMB_%s.ans" % (cluster_id[i])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='TaskNodesRunning',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': cluster_id[i]
                                                        },
                                                    ],
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("TaskNodesRunning Metrcs obtained for cluster %s " % cluster_id[i])
            logger.debug(response)
            response = json.dumps(response, indent=4, default=json_util.default)
            print "resp=", response
            file_name = dir + "/" + "TaskNodesRunning_%s.ans" % (cluster_id[i])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            response = client.get_metric_statistics(Namespace="AWS/ElasticMapReduce",
                                                    MetricName='CoreNodesRunning',
                                                    Dimensions=[
                                                        {
                                                            'Name': 'JobFlowId',
                                                            'Value': cluster_id[i]
                                                        },
                                                    ],
                                                    StartTime=startTime,
                                                    EndTime=endTime,
                                                    Period=300,
                                                    Statistics=[
                                                        'Average', 'Minimum', 'Maximum',
                                                    ],
                                                    )

            logger.debug("CoreNodesRunning Metrics obtained for cluster %s " % cluster_id[i])
            logger.debug(response)
            response = json.dumps(response, indent=4, default=json_util.default)
            print "resp", response
            file_name = dir + "/" + "CoreNodesRunning_%s.ans" % (cluster_id[i])
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write(str(response))
            else:
                with open(file_name, 'a') as f:
                    f.write(str(response))

            if end_time[i] == 1:
                diff = datetime.now() - startTime
            else:
                diff = end_time[i] - startTime
            days_left = min(5, diff.days)
            logger.debug("Days left = %s" % days_left)
            print "daysleft =", days_left
            startTime = startTime + timedelta(days=days_left)

    zipfile = 'emr_metrics_%s' % email
    shutil.make_archive(zipfile, 'zip', './emr_metrics')
    shutil.rmtree('./emr_metrics')

    logger.info("Zip file of all cluster's metrics is created successfully in the same location")
    return {"message": "files created successfully in the same location", "status": "success"}


if __name__ == "__main__":
    cloudwatch_metric()
