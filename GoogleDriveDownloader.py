import os
# comment out if don't need proxy
os.environ['HTTP_PROXY']="http://127.0.0.1:10809"
os.environ['HTTPS_PROXY']="http://127.0.0.1:10809"

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from tqdm import tqdm
from multiprocess import Pool
from datetime import datetime


class GoogleDriveDownloader:
    """A customized class to download files from Google Drive
    """    
#     https://pythonhosted.org/PyDrive/filemanagement.html#download-file-content
#     注意这里是用的v2的api, https://developers.google.com/drive/api/v2/search-files
    def __init__(self, delete=False, overwrite=False):
        """Initialization

        Args:
            delete (bool, optional): [whether to delete file after finishing download]. Defaults to False.
            overwrite (bool, optional): [whether to overwrite local file with the same name]. Defaults to False.
        """        
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth() 
        self.drive = GoogleDrive(gauth)
        self.delete = delete
        self.overwrite = overwrite
        
        
    def refresh_auth(self):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth() 
        self.drive = GoogleDrive(gauth)
        
        
    def search_target_folder(self, foldername):
        """Search a target folder with folder name

        Args:
            foldername ([str]): [folder name]

        Returns:
            [str]: [id of the target folder]
        """        
        file_list = self.drive.ListFile({'q': f"title contains '{foldername}' and trashed=false and mimeType='application/vnd.google-apps.folder'"}).GetList()
        return file_list[0]['id']
    
    
    def search_target_file(self, filename, mimeType=None):
        """Search a target file with file name and mimeType

        Args:
            filename ([str]): [file name]
            mimeType ([str], optional): [mimeType]. Defaults to None.

        Returns:
            [str]: [id of the target file]
        """        
        if mimeType:
            file_list = self.drive.ListFile({'q': f"title contains '{filename}' and trashed=false and mimeType='{mimeType}'"}).GetList()
        else:
            file_list = self.drive.ListFile({'q': f"title contains '{filename}' and trashed=false"}).GetList()
        return file_list[0]['id']
    
    
    def download_target_file(self, file_id, dst_name):
        """Download single file with id

        Args:
            file_id ([str]): [file id]
            dst_name ([str]): [local file name]
        """        
        # windows仅支持继承必要的资源，无法继承全部资源，所以这里要单独导入os
        # https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
        import os

        f = self.drive.CreateFile({'id': file_id})
        if os.path.isfile(dst_name) and (not self.overwrite):
            print(f'{dst_name} exists, skip it')
        else:
            f.GetContentFile(dst_name)
            print(f'{dst_name} download finished')
            
        if self.delete:
            f.Delete()
    
    
    def list_files_in_folder(self, folder_id):
        """List files in a folder

        Args:
            folder_id ([str]): [folder id]

        Returns:
            [list]: [file list]
        """        
        file_list = self.drive.ListFile({'q': f"'{folder_id}' in parents and mimeType!='application/vnd.google-apps.folder'"}).GetList()
        return file_list
    
    
    def download_file_list(self, file_list, dst_dir):
        """Download file list

        Args:
            file_list ([list]): [file list]
            dst_dir ([str]): [local target dir]
        """        
        for f in tqdm(file_list):
            dst_name = os.path.join(dst_dir, f['title'])
            self.download_target_file(f['id'], dst_name)
           
    
    def download_file_list_parallel(self, file_list, dst_dir, processes=4):
        """Download file list using multiprocess

        Args:
            file_list ([list]): [file list]
            dst_dir ([str]): [local target dir]
            processes (int, optional): [number of processes to use]. Defaults to 4.
        """        
#         # https://stackoverflow.com/a/65001152/6103796, multiprocessing doesn't work in ipython，replace it with multiprocess
#         from multiprocessing import Pool          
        from multiprocess import Pool
        arg_list = [(f['id'], os.path.join(dst_dir, f['title'])) for f in file_list]
        with Pool(processes) as p:
            p.starmap(self.download_target_file, arg_list)
            
    
    def download_files_in_folder(self, foldername, dst_dir, parallel=True):
        """Download files in a target folder

        Args:
            foldername ([str]): [folder name]
            dst_dir ([str]): [local target dir]
            parallel (bool, optional): [whether to use multiprocess]. Defaults to True.
        """        
        folder_id = self.search_target_folder(foldername)
        file_list = self.list_files_in_folder(folder_id)
        if parallel:
            self.download_file_list_parallel(file_list, dst_dir)
        else:
            self.download_file_list(file_list, dst_dir)
    
        if self.delete:
            f = self.drive.CreateFile({'id': folder_id})
            f.Delete()
            
    def download_files_in_folder_batch(self, foldername, dst_dir, parallel=True, batch_size=4):
        """Download many files in a target folder in batch

        Args:
            foldername ([str]): [folder name]
            dst_dir ([str]): [local target dir]
            parallel (bool, optional): [whether to use multiprocess]. Defaults to True.
            batch_size (int, optional): [batch size]. Defaults to 4.
        """        
        folder_id = self.search_target_folder(foldername)
        file_list = self.list_files_in_folder(folder_id)

        for i in tqdm(range(0, len(file_list), batch_size)):
            batch_list = file_list[i:i+batch_size]
            print("start to download: \n%s"%('\n'.join([f['title'] for f in batch_list])))

            self.refresh_auth()
            if parallel:
                self.download_file_list_parallel(batch_list, dst_dir, processes=batch_size)
            else:
                self.download_file_list(batch_list, dst_dir)
            print(f'batch {i} finished')

        print('task finished')

            
    def recursively_download_files_in_folder(self, drive_folder, dst_dir):
        """Recursively download all files in a target folder, very slow when there are many subfolders

        Args:
            drive_folder ([str]): [folder name]
            dst_dir ([str]): [local target dir]
        """        
        # reference: https://medium.datadriveninvestor.com/recursively-download-all-the-contents-of-a-google-drive-folder-using-python-wget-and-a-bash-script-d8f2c6b105d5
        # 将 drive_folder 及其所有子文件夹中的 file 都下载到 dst_dir 中
        # 这个方法在子文件夹特别多的时候会很慢
        parent_folder_id = self.search_target_folder(drive_folder)

        folder_queue = [parent_folder_id]
        dir_queue = [drive_folder]
        while len(folder_queue) != 0:
            current_folder_id = folder_queue.pop(0)
            folder_list = self.drive.ListFile({'q': f"'{current_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"}).GetList()

            current_parent = dir_queue.pop(0)
            current_parent = current_parent if current_parent[-1]=='/' else current_parent+'/'
            for folder in folder_list:
                folder_queue.append(folder['id'])
                dir_queue.append(current_parent + folder['title'])
                print(current_parent + folder['title'])
                
                file_list = self.list_files_in_folder(folder['id'])
                if len(file_list) > 0:
                    self.download_file_list_parallel(file_list, dst_dir)
