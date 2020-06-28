# -*- codeing: utf-8 -*-
"""Classes for S3 Buckets"""
from pathlib import Path
import mimetypes
from botocore.exceptions import ClientError


class BucketManager:
    """Manage and S3 Bucket."""

    def __init__(self, session):
        """Create a BucketManager object."""
        self.session = session
        self.s3 = self.session.resource('s3')
        pass

    def all_buckets(self):
        return self.s3.buckets.all()
        print(obj)

    def all_objects(self, bucket_name):
        """Get all objects for all buckets"""
        return self.s3.Bucket(bucket_name).objects.all()
        print(obj)

    def init_bucket(self, bucket_name):
        """Create new bucket or return existing bucket"""
        s3_bucket = None
        try:
            s3_bucket = self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                    }
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'BucketAlreadyOpenedByYou':
                s3_bucket = self.s3.Bucket(bucket_name)
            else:
                raise error

    def set_policy(self, bucket):
        """Set bucket policy readable by everyone"""
        policy = """
        {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    "arn:aws:s3:::%s/*"
                ]
            }
        ]
        }
        """ % bucket_name

        policy = policy.strip()

        pol = bucket.Policy()
        pol.put(Policy=policy)

    def configure_website(self, bucket):
        bucket.Website().put(WebsiteConfiguration={
            'ErrorDocument': {
                'Key': 'error.html'
            },
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        })

    def upload_file(self, bucket, path, key):
        """Upload path to s3_bucket at key."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'

        return bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
            })

    def sync(self, pathname, bucket_name):
        """Sync local folder to S3"""
        bucket = self.s3.Bucket(bucket_name)
        root = Path(pathname).expanduser().resolve()

        def handle_directory(target):
            for p in target.iterdir():
                if p.is_dir():
                    handle_directory(p)
                if p.is_file():
                    self.upload_file(bucket, str(p), str(p.relative_to(root)))

        handle_directory(root)
