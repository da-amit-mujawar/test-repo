import io
import os
import zipfile
from contextlib import contextmanager
from typing import Any, Optional, List, Generator
from zipfile import BadZipFile

import boto3


class SeekableBufferedS3File(io.RawIOBase):

    def __init__(self, s3_object, buffer_size: int):
        self.s3_object = s3_object
        self.size = self.s3_object.content_length
        self.current_pos = 0
        self.window = (0, 0)
        self.buffer_size = buffer_size
        self.buffer = None
        self.buffer_view: Optional[bytes] = None

    def seek(self, offset: int, whence: int = io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.current_pos = offset
        elif whence == io.SEEK_CUR:
            self.current_pos += offset
        elif whence == io.SEEK_END:
            self.current_pos = self.s3_object.content_length + offset
        else:
            raise Exception(f"Invalid whence value: {whence}")
        return self.current_pos

    def read(self, size=-1) -> bytes:
        if size > self.buffer_size:
            size = self.buffer_size
        fr = self.current_pos
        if size == -1:
            to = self.size
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            if fr + size >= self.size:
                return self.read()
            to = fr + size
            self.seek(offset=size, whence=io.SEEK_CUR)
        if fr < self.window[0] or to > self.window[1]:
            read_fr = fr
            if fr + self.buffer_size > self.size:
                read_to = self.size
            else:
                read_to = fr + self.buffer_size
            self.window = (read_fr, read_to)
            self.buffer = self.s3_object.get(Range=f"bytes={read_fr}-{read_to}")["Body"].read()
            self.buffer_view = memoryview(self.buffer)  # type: ignore
        assert self.buffer_view is not None
        return bytes(self.buffer_view[fr - self.window[0]: to - self.window[0]])

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True


class S3ZipFile:

    def __init__(self, bucket: str, zipfile_key: str, buffer_size: int):
        self.bucket = bucket
        self.zipfile_key = zipfile_key
        self.buffer_size = buffer_size
        self.remote_file = None
        self.s3_resource: Any = None

    def open(self):
        self.s3_resource = boto3.resource('s3')
        file_obj = self.s3_resource.Object(bucket_name=self.bucket, key=self.zipfile_key)
        if file_obj.content_length == 0:
            raise BadZipFile(f"Zip file {self.bucket}/{self.zipfile_key} length cannot be 0")
        elif file_obj.content_length < self.buffer_size:
            self.remote_file = io.BytesIO(file_obj.get()["Body"].read())
            self.remote_file.seek(0)
            self.zipf = zipfile.ZipFile(self.remote_file, mode='r')
        else:
            self.zipf = zipfile.ZipFile(SeekableBufferedS3File(file_obj, self.buffer_size), mode='r')
        return self

    def close(self):
        self.zipf.close()
        if self.remote_file:
            self.remote_file.close()

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_filenames(self) -> List[str]:
        return [f.filename for f in self.zipf.infolist() if not f.is_dir()]

    def get_filesize(self, filename: str) -> int:
        return self.zipf.getinfo(filename).file_size

    @contextmanager
    def open_text_file(self, filename: str) -> Generator[io.TextIOWrapper, None, None]:
        with self.zipf.open(filename, 'r') as f:
            yield io.TextIOWrapper(f, newline=None)

    @contextmanager
    def extract(self, filename, file_dest_path_key: str):
        file_dest_full_path = file_dest_path_key + os.path.sep + filename
        with self.zipf.open(filename, 'r') as f:
            self.s3_resource.meta.client.upload_fileobj(
                f,
                Bucket=self.bucket,
                Key=f'{file_dest_full_path}'
            )
