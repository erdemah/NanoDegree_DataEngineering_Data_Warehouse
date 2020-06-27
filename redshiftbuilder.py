import boto3
import json
from time import sleep
import configparser


class RedshiftBuilder:

    def __init__(self, config_parser):
        self.config = config_parser
        self.key = config_parser.get('AWS', 'KEY')
        self.secret = config_parser.get('AWS', 'SECRET')
        self.dwh_cluster_type = config_parser.get('DWH', 'DWH_CLUSTER_TYPE')
        self.dwh_num_nodes = config_parser.get('DWH', 'DWH_NUM_NODES')
        self.dwh_node_type = config_parser.get('DWH', 'DWH_NODE_TYPE')
        self.dwh_cluster_identifier = config_parser.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        self.dwh_db = config_parser.get("DWH", "DWH_DB")
        self.dwh_db_user = config_parser.get("DWH", "DWH_DB_USER")
        self.dwh_db_password = config_parser.get("DWH", "DWH_DB_PASSWORD")
        self.dwh_port = config_parser.get("DWH", "DWH_PORT")
        self.security_group_id = config_parser.get("EC2", "SECURITY_GROUP_ID")
        self.dwh_iam_role_name = config_parser.get("DWH", "DWH_IAM_ROLE_NAME")
        self.role_arn = None
        self.iam_client = None
        self.redshift_client = None
        self.ec2_client = None
        self.dwh_end_point = None
        self.cluster_props = None

    def redshift_client_create(self):
        """
        Helper functions that creates redshift client
        :return: None
        """
        redshift = boto3.client("redshift", region_name="us-west-2",
                                aws_access_key_id=self.key, aws_secret_access_key=self.secret)
        self.redshift_client = redshift

    def iam_client_create(self):
        """
        Helper functions that creates iam client
        :return: None
        """
        iam = boto3.client('iam', aws_access_key_id=self.key,
                           aws_secret_access_key=self.secret, region_name='us-west-2')
        self.iam_client = iam

    def ec2_client_create(self):
        """
        Helper functions that creates ec2 client
        :return: None
        """
        ec2 = boto3.resource('ec2', region_name="us-west-2",
                             aws_access_key_id=self.key, aws_secret_access_key=self.secret)
        self.ec2_client = ec2

    def create_iam_role(self):
        """
        Creates IAM role and write the arn role to dwh.cfg
        :return: None
        """
        # 1.1 Create the role,
        try:
            print("1.1 Creating a new IAM Role")
            self.iam_client.create_role(
                Path='/',
                RoleName=self.dwh_iam_role_name,
                Description="Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                    'Effect': 'Allow',
                                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )
        except Exception as e:
            print(e)
        print("1.2 Attaching Policy")
        # Attaching s3 ReadOnly Policy
        self.iam_client.attach_role_policy(RoleName=self.dwh_iam_role_name,
                                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print("1.3 Get the IAM role ARN")
        role_arn = self.iam_client.get_role(RoleName=self.dwh_iam_role_name)['Role']['Arn']
        self.role_arn = role_arn
        # self.config["IAM_ROLE"] = {"ARN": role_arn}
        self.config.set("IAM_ROLE", "arn", role_arn)
        with open("dwh.cfg", "w+") as config_file:
            self.config.write(config_file)

    def create_redshift_cluster(self):
        """
        Initiates redshift cluster
        :return: None
        """
        try:
            self.redshift_client.create_cluster(
                # HardWare
                ClusterType=self.dwh_cluster_type,
                NodeType=self.dwh_node_type,
                NumberOfNodes=int(self.dwh_num_nodes),

                # Identifiers & Credentials
                DBName=self.dwh_db,
                ClusterIdentifier=self.dwh_cluster_identifier,
                MasterUsername=self.dwh_db_user,
                MasterUserPassword=self.dwh_db_password,
                VpcSecurityGroupIds=[self.security_group_id],
                # Roles (for s3 access)
                IamRoles=[self.role_arn]
            )
        except Exception as e:
            print(e)
        # Checks whether cluster is ready or not each minute: 10 Minutes in total
        for i in range(10):
            sleep(60)
            print("Awaiting for cluster to start up")
            self.cluster_props = \
                self.redshift_client.describe_clusters(ClusterIdentifier=self.dwh_cluster_identifier)['Clusters'][0]
            if self.cluster_props['ClusterStatus'] == 'available' and \
                    self.cluster_props['ClusterAvailabilityStatus'] == 'Available':
                # When the cluster status becomes "Available"
                self.dwh_end_point = self.cluster_props['Endpoint']['Address']
                self.config.set("DWH", "host", self.dwh_end_point)
                with open("dwh.cfg", "w+") as config_file:
                    self.config.write(config_file)
                break

    def open_tcp_port(self):
        """
        Adds an inbound rule to the security group that redshift cluster is using
        :return: None
        """
        try:
            vpc = self.ec2_client.Vpc(id=self.cluster_props['VpcId'])
            sg_list = list(vpc.security_groups.all())
            for sg in sg_list:
                if sg.group_id == self.security_group_id:
                    sg.authorize_ingress(
                        GroupName=sg.group_name,
                        CidrIp='0.0.0.0/0',
                        IpProtocol='TCP',
                        FromPort=int(self.dwh_port),
                        ToPort=int(self.dwh_port))
                continue
        except Exception as e:
            print(e)

    def remove_cluster(self):
        """
        Removes redshift cluster
        :return: None
        """
        self.redshift_client.delete_cluster(ClusterIdentifier=self.dwh_cluster_identifier,
                                            SkipFinalClusterSnapshot=True)

    def remove_iam(self):
        """
        Removes the iam role
        :return: None
        """
        # detaching s3 read policy from the iam role
        self.iam_client.detach_role_policy(RoleName=self.dwh_iam_role_name,
                                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        # removing the iam role
        self.iam_client.delete_role(RoleName=self.dwh_iam_role_name)

    def remove_sg_inbound_rule(self):
        """
        Deletes the tcp inbound rule given to redshift cluster
        :return: None
        """
        try:
            vpc = self.ec2_client.Vpc(id=self.cluster_props['VpcId'])
            sg_list = list(vpc.security_groups.all())
            for sg in sg_list:
                if sg.group_id == self.security_group_id:
                    sg.authorize_ingress(
                        GroupName=sg.group_name,
                        CidrIp='0.0.0.0/0',
                        IpProtocol='TCP',
                        FromPort=int(self.dwh_port),
                        ToPort=int(self.dwh_port))
                continue
        except Exception as e:
            print(e)

    def clean_up_cluster(self):
        """
        Clean-up procedures start from this call
        :return: None
        """
        self.remove_cluster()
        self.remove_iam()
        self.remove_sg_inbound_rule()

    def build_cluster(self):
        """
        Building the cluster starts from this call
        :return: None
        """
        self.redshift_client_create()
        self.iam_client_create()
        self.ec2_client_create()
        self.create_iam_role()
        # self.update_iam_config()
        self.create_redshift_cluster()
        # uses created redshift cluster's vpc_id
        self.open_tcp_port()

    def get_dbc_access_parameters(self):
        """
        Returns the necessary parameters to be used to load, insert and query the database
        :return: Database access parameters
        """
        params = "host={} dbname={} user={} password={} port={}". \
            format(self.dwh_end_point, self.dwh_db, self.dwh_db_user, self.dwh_db_password, self.dwh_port)
        return params
