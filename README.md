# Google Drive Downloader

My customized class based on PyDrive to download files from Google Drive.

## Environment

```bash
pip install -r requirements.txt
```

Following [here](https://pythonhosted.org/PyDrive/quickstart.html) to configure authentication.

## Use case

* download a single file
  
```python
from GoogleDriveDownloader import GoogleDriveDownloader
downloader = GoogleDriveDownloader()

fid = downloader.search_target_file('filename')
downloader.download_target_file(fid, 'local/dir/filename')
```

* download all files in a folder from Google Drive

```python
download_dir = 'local/dir'
foldername = 'foldername'
downloader.download_files_in_folder(foldername, download_dir, parallel=True)
```

* download in batch if there are lots of files in the folder

```python
downloader.download_files_in_folder_batch(foldername, download_dir, parallel=True)
```
