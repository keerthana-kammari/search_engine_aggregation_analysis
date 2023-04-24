from aws_cdk import Stack
from aws_cdk import Aws
from constructs import Construct
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk.aws_s3 import Bucket
import aws_cdk.aws_s3_deployment as s3_deployment

class ParseSearchHitsJob(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, parameters:dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create an unique IAM role to provide permissions on the glue job
        glue_job_role = iam.Role(
                    self,
                    'GlueJobRole',
                    assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
                    description="Role to parse data used by glue job",
                    role_name='parse_source_data_role'
                )
        # exposing the IAM role to be used by other resources
        self.glue_job_role = glue_job_role

        # assign s3 list, get and put permissions to the IAM role
        glue_job_role.add_to_principal_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                                  actions=["s3:List*",
                                                                            "s3:Get*",
                                                                            "s3:Put*",
                                                                            "s3:DeleteObject"],
                                                                  resources=[f"arn:aws:s3:::{parameters['bucket_name']}",
                                                                             f"arn:aws:s3:::{parameters['bucket_name']}/*",
                                                                             f"arn:aws:s3:::{parameters['scripts_bucket']}",
                                                                             f"arn:aws:s3:::{parameters['scripts_bucket']}/*"]))
        
        # decrypt the s3 buckets using the KMS key
        glue_job_role.add_to_principal_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                                  actions=["kms:Decrypt",
                                                                            "kms:Encrypt",
                                                                            "kms:GenerateDataKey"],
                                                                  resources=[f"arn:aws:kms:{Aws.REGION}:{Aws.ACCOUNT_ID}:key/{parameters['key1']}",
                                                                             f"arn:aws:kms:{Aws.REGION}:{Aws.ACCOUNT_ID}:key/{parameters['key2']}"]))
        # allow IAM role to write cloud watch metrics
        glue_job_role.add_to_principal_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                                  actions=["cloudwatch:PutMetricData"],
                                                                  resources=["*"]))
        
        # allow IAM role to create and add log stream cloud watch
        glue_job_role.add_to_principal_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                                  actions=["logs:CreateLogGroup",
                                                                            "logs:CreateLogStream",
                                                                            "logs:PutLogEvents"],
                                                                  resources=["arn:aws:logs:*:*:/aws-glue/*"]))
   
        job_params = {
                        "--enable-glue-datacatalog": True,
                        "--enable-job-insights": True,
                        "--enable-metrics": True,
                        "--enable-continuous-cloudwatch-log": True,
                        "--job-language": "python"
                        ""
                        }
        # creates a glue job
        glue_job = glue.CfnJob(self,"GlueJob",
                                role=glue_job_role.role_arn,
                                allocated_capacity=10,
                                glue_version="3.0",
                                command=glue.CfnJob.JobCommandProperty(
                                                                        name="glueetl",
                                                                        python_version="3",
                                                                        script_location=f"s3://{parameters['scripts_bucket']}/scripts/parse_search_hits_script.py"
                                                                        ),
                                description="Glue job to parse the source raw data into a structured format",
                                default_arguments=job_params,
                                name="parse_search_engine_hits_job",
                                max_retries=0)

        # expose glue job to extract its properties
        self.glue_job = glue_job

        # upload scripts to s3
        script_bucket = Bucket.from_bucket_name(self, "ScriptsBucket", parameters["scripts_bucket"])
        s3_deployment.BucketDeployment(self, "DeployGlueJobScript", 
                                        sources=[s3_deployment.Source.asset("./scripts")],
                                        destination_bucket=script_bucket,
                                        destination_key_prefix="scripts")
