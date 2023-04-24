from aws_cdk import App
from stacks.parse_search_hits_job import ParseSearchHitsJob
from stacks.s3_bucket_stack import S3BucketStack

app = App()

class search_eng_analysis(App):

    def __init__(self) -> None:
        super().__init__()

        s3_bucket_stack = S3BucketStack(self,  "S3BucketStack",
                                        parameters={"bucket_name": "shopzilla-search-eng-hits"})
        
        s3_script_bucket = S3BucketStack(self, "S3ScriptsStack",
                                         parameters={"bucket_name": "shopzilla-scripts-bucket"})
        # print(s3_bucket_stack.bucket)
        glue_job_stack = ParseSearchHitsJob(self, "GlueJobStack",
                                        parameters={"bucket_name": s3_bucket_stack.bucket.bucket_name,
                                                    "scripts_bucket": s3_script_bucket.bucket.bucket_name,
                                                    "key1": "0345efe6-ac21-4149-bebf-bb675dc99572", 
                                                    "key2": "af7db642-1f5c-4f03-86a3-aa79f9cfeaf9"
                                                    })
        
        glue_job_stack.node.add_dependency(s3_bucket_stack, s3_script_bucket)

# if called standalone synthesize
search_eng_analysis().synth()