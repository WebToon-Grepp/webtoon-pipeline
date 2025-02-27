from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFoldersystemToS3Operator(BaseOperator):
    template_fields: Sequence[str] = ("folder", "dest_bucket")

    def __init__(
        self,
        *,
        folder: str,
        folder_key: str,
        dest_bucket: str,
        aws_conn_id: str | None = "aws_default",
        verify: str | bool | None = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.folder = folder
        self.folder_key = folder_key
        self.dest_bucket = dest_bucket
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

        self.collect()

    def collect(self):
        filenames = []
        for root, dirs, files in os.walk(self.folder):
            for file in files:
                filename = os.path.join(root, file)
                filenames.append(filename)
        self.filenames = filenames

    def execute(self, context: Context):
        for filename in self.filenames:
            print(filename)
            self.dest_key = filename.replace(self.folder_key, "").lstrip("/")
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
            s3_bucket, s3_key = s3_hook.get_s3_bucket_key(
                self.dest_bucket, self.dest_key, "dest_bucket", "dest_key"
            )
            s3_hook.load_file(
                filename,
                s3_key,
                s3_bucket,
                self.replace,
                self.encrypt,
                self.gzip,
                self.acl_policy,
            )
