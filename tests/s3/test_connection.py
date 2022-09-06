import pytest
import random
import shutil
from tests.config import config



@pytest.mark.parametrize('zip', [False, True])
def test_s3(s3, zip):
    """Tests all S3Connection methods."""

    # Get directory prefixes.
    dir_local = config.paths.tests / 'test_s3' / str(zip)
    dir_remote = f'test_chris/aws_utils/test_s3/{zip}'

    # Get file paths.
    paths = {
        'local_1': dir_local / 'test_1.txt',
        'local_2': dir_local / 'test_2.txt',
        'remote_1': dir_remote + '/test_1.txt' + ('.gz' if zip else ''),
        'remote_2': dir_remote + '/test_2.txt' + ('.gz' if zip else ''),
        'remote_3': dir_remote + '/test_3.txt' + ('.gz' if zip else ''),
    }

    # Generate a random message.
    message = f'Hello world!  random = {random.random()}.'

    # Create a local test file.
    dir_local.mkdir(parents=True, exist_ok=True)
    with open(paths['local_1'], 'w') as file:
        file.write(message)

    # Delete any existing files on S3.
    s3.delete_at(dir_remote)

    # Upload 1.
    s3.upload(paths['local_1'], paths['remote_1'], zip=zip)

    # Copy 1 to 2.
    s3.copy(paths['remote_1'], paths['remote_2'])

    # Move 1 to 3.
    s3.move(paths['remote_1'], paths['remote_3'])

    # Verify only 2 and 3 exists.
    files = s3.list(dir_remote)
    assert (
        any(x['Key'] == paths['remote_2'] and x['Size'] > 0 for x in files) and
        any(x['Key'] == paths['remote_3'] and x['Size'] > 0 for x in files) and
        len(files) == 2
    )

    # Download 2.
    s3.download(paths['remote_2'], paths['local_2'], unzip=zip)

    # Verify 2 is identical to 1.
    with open(paths['local_2'], 'r') as file:
        s = file.read()
        assert s == message

    # Clean up local.
    shutil.rmtree(dir_local, ignore_errors=True)

    # Clean up remote.
    s3.delete([paths['remote_2'], paths['remote_3']])

    # Verify clean up.
    assert len(s3.list(dir_remote)) == 0
