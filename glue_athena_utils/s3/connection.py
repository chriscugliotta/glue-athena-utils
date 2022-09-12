import boto3
import botocore
import gzip
import logging
import shutil
from pathlib import Path
from typing import Dict, List
log = logging.getLogger(__name__)



class S3Connection:
    """
    An S3 interface with many convenience functions.

    Conveniences include:
        - Pagination wrangling, e.g. listing or deleting more than 1000 objects.
        - Automated (and optional) gzipping on upload.
        - Automated (and optional) unzipping on download.

    Attributes:
        bucket_name (str):
            S3 bucket name.

        _session (boto3.session.Session):
            Internal boto3 Session object.

        _client (botocore.client.S3):
            Internal boto3 S3 client object.

    References:
        boto3 S3 client:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """
    def __init__(self, bucket_name: str, profile_name: str = None, region_name: str = 'us-east-1', boto3_session: object = None, proxy_url: str = None):
        self.bucket_name: str = bucket_name
        self._session: object = self._get_boto3_session(profile_name, region_name, boto3_session)
        self._client: object = self._session.client('s3', config=self._get_proxy_config(proxy_url))
        log.info(f'Constructed new S3Connection!  bucket_name = {self.bucket_name}, profile_name = {self._session.profile_name}.')

    def _get_boto3_session(self, profile_name: str, region_name: str, boto3_session: object) -> object:
        if boto3_session is not None:
            return boto3_session
        else:
            return boto3.session.Session(profile_name=profile_name, region_name=region_name)

    def _get_proxy_config(self, proxy_url: str) -> object:
        if proxy_url is not None:
            return botocore.config.Config(proxies={'https': proxy_url})

    def list(self, prefix: str) -> List[Dict]:
        """
        Queries an S3 bucket, and returns all keys starting with the given prefix.

        Args:
            prefix (str):
                Search prefix.

        Returns:
            List[Dict]:  A list containing metadata (e.g. Key, Size, LastModified) for each object.
        """
        result = []
        pages = self._client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket_name, Prefix=prefix)
        for page in pages:
            if 'Contents' in page:
                result.extend(page['Contents'])
        return result

    def upload(self, local: Path, remote: str, zip: bool = False, quiet: bool = False):
        """
        Uploads a local file to S3.

        Args:
            local (Path):
                Local file to upload, e.g. `Path('C:/file.txt')`.

            remote (str):
                Destination S3 path, e.g. `pa/file.txt`.

            zip (bool):
                If true, file is zipped prior to upload (then local zip is deleted).

        Note:
            When `zip=True`, the `remote` arg should have `.gz` file extension.
        """
        if zip:
            local = self._to_gzip(local)
        if not quiet:
            log.debug(f'Uploading:  {remote}.')
        self._upload(local, remote)
        if zip:
            local.unlink()

    def _upload(self, local: Path, remote: str):
        self._client.upload_file(
            Filename=str(local),
            Bucket=self.bucket_name,
            Key=remote,
        )

    def download(self, remote: str, local: Path, unzip: bool = False, quiet: bool = False):
        """
        Downloads a file from S3.

        Args:
            remote (str):
                S3 file to download, e.g. `pa/file.txt`.

            local (Path):
                Destination local file path, e.g. `Path('C:/file.txt')`.

            unzip (bool):
                If true, file is unzipped after download (then local zip is deleted).

        Note:
            When `unzip=True`, the `remote` arg should have `.gz` file extension.
        """
        if unzip:
            local = local.parent / (local.name + '.gz')
        if not quiet:
            log.debug(f'Downloading:  {remote}.')
        self._download(remote, local)
        if unzip:
            self._from_gzip(local)
            local.unlink()

    def _download(self, remote: str, local: Path):
        self._client.download_file(
            Bucket=self.bucket_name,
            Key=remote,
            Filename=str(local),
        )

    def delete(self, keys: List[str], chunk_size: int = 1000, safe_word: str = 'chris/', quiet: bool = False):
        """
        Deletes a list of objects from S3.

        Args:
            keys (List[str]):
                List of keys to delete.

            chunk_size (int):
                Number of objects deleted per boto3 API call.  (boto3 limit is 1000.)

            safe_word (str):
                A safety feature intended to prevent catastrophic, accidental deletion of data.  If
                any given key does NOT contain this substring, an exception is raised.
        """
        for key in keys:
            if safe_word not in key:
                raise Exception(f'Unexpected S3 delete path:  {key}, safe_word = {safe_word}.')
        if not quiet:
            if len(keys) == 1:
                log.debug(f'Deleting:  {keys[0]}.')
            else:
                log.debug(f'Deleting {len(keys):,} objects from S3.')
        chunks = self._get_chunks(keys, chunk_size)
        for chunk in chunks:
            self._delete(chunk)

    def _delete(self, keys: List[str]):
        self._client.delete_objects(
            Bucket=self.bucket_name,
            Delete={'Objects': [{'Key': x} for x in keys]},
        )

    def delete_at(self, prefix: str, quiet: bool = False) -> List[Dict]:
        """Deletes all objects at the given S3 prefix."""
        objects = self.list(prefix)
        size = sum(x['Size'] for x in objects) / 1024 / 1024
        if not quiet:
            log.debug(f'Deleting {len(objects):,} object(s), {size:0.2f} MiB at:  {prefix}.')
        self.delete([x['Key'] for x in objects], safe_word=prefix, quiet=True)
        return objects

    def copy(self, source: str, dest: str, quiet: bool = False):
        if not quiet:
            log.debug(f'Copying {source} to {dest}.')
        self._client.copy(
            CopySource={'Bucket': self.bucket_name, 'Key': source},
            Bucket=self.bucket_name,
            Key=dest,
        )

    def move(self, source: str, dest: str, safe_word: str = 'chris/'):
        self.copy(source, dest)
        self.delete([source], safe_word=safe_word)

    def _to_gzip(self, file: Path) -> Path:
        """Gzips a file, and appends `.gz` to its file path."""
        assert file.suffix != '.gz'
        path_unzip = file
        path_zip = file.parent / (file.name + '.gz')
        with open(path_unzip, 'rb') as file_input:
            with gzip.open(path_zip, 'wb') as file_output:
                shutil.copyfileobj(file_input, file_output)
        return path_zip

    def _from_gzip(self, file: Path) -> Path:
        """Unzips a gzip, and removes `.gz` from its file path."""
        assert file.suffix == '.gz'
        path_zip = file
        path_unzip = file.parent / file.stem
        with gzip.open(path_zip, 'rb') as file_input:
            with open(path_unzip, 'wb') as file_output:
                shutil.copyfileobj(file_input, file_output)
        return path_unzip

    def _get_chunks(self, l: List, n: int) -> List[List]:
        """
        Divides a list into chunks of size `n`.

        References:
            Code taken from:
            https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
        """
        return [l[i:i+n] for i in range(0, len(l), n)]



class MockS3Connection(S3Connection):
    """
    Mock S3 connection.  Can be used by tests.

    Attributes:
        dir (Path):
            Local directory serving as mock S3 file store.
    """

    def __init__(self, dir: Path):
        self.dir = Path(dir)
        self.bucket_name = 'mock'
        log.info(f'Constructed new MockS3Connection!  dir = {dir}.')

    def list(self, prefix: str) -> List[Dict]:
        results = []
        for path in self.dir.rglob('*'):
            if path.is_file():
                relative_path = path.relative_to(self.dir).as_posix()
                if relative_path.startswith(prefix):
                    results.append({
                        'Key': relative_path,
                        'Size': path.stat().st_size,
                    })
        return results

    def _upload(self, local: Path, remote: str):
        remote = self.dir / remote
        remote.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(local, remote)

    def _download(self, remote: str, local: Path):
        remote = self.dir / remote
        shutil.copy(remote, local)

    def _delete(self, keys: List[str]):
        for key in keys:
            remote = self.dir / key
            if remote.is_file():
                remote.unlink()

    def copy(self, source: str, dest: str, quiet: bool = False):
        if not quiet:
            log.debug(f'Copying {source} to {dest}.')
        source = self.dir / source
        dest = self.dir / dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, dest)
