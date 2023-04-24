from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_s3 as s3

class S3BucketStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, parameters:dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        """Create an S3 bucket with a provided bucket name"""

        bucket = s3.Bucket(self, "S3Stack", 
                           bucket_name=parameters["bucket_name"])
        
        # exposing bucket to extract its properties
        self.bucket = bucket