"""Unit tests for the idempotent S3 upload logic in pipeline.ingest (no AWS needed)."""
from botocore.exceptions import ClientError

from pipeline import ingest


class FakeS3:
    """Just enough of the boto3 S3 client for upload_if_changed()."""

    def __init__(self):
        self.objects = {}   # key -> metadata
        self.uploads = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"Metadata": self.objects[Key]}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploads += 1
        self.objects[Key] = (ExtraArgs or {}).get("Metadata", {})


def test_upload_then_skip_on_rerun(tmp_path):
    f = tmp_path / "file.parquet"
    f.write_bytes(b"taxi data v1")
    s3 = FakeS3()

    assert ingest.upload_if_changed(s3, str(f), "b", "raw/k.parquet", "http://x") is True
    assert ingest.upload_if_changed(s3, str(f), "b", "raw/k.parquet", "http://x") is False  # re-run
    assert s3.uploads == 1
    assert list(s3.objects) == ["raw/k.parquet"]


def test_changed_file_is_replaced_not_duplicated(tmp_path):
    f = tmp_path / "file.parquet"
    s3 = FakeS3()
    f.write_bytes(b"v1")
    ingest.upload_if_changed(s3, str(f), "b", "raw/k.parquet", "http://x")
    f.write_bytes(b"v2 - TLC republished the file")
    assert ingest.upload_if_changed(s3, str(f), "b", "raw/k.parquet", "http://x") is True
    assert s3.uploads == 2
    assert len(s3.objects) == 1   # same key overwritten, still one object
