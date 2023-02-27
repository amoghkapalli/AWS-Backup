# AWS-Backup
Run the program in the same directory as where backup.py is located.
To backup, from a local directory to a s3 bucket, enter the following command:

'''python backup.py backup local-directory bucket-name::folderName'''
 
The local directory must be the entire path to folder or file directory from root. A . can also be inputted for the local-directory and all the files in the current directory will be uploaded to the cloud.
 
To restore files from a s3 bucket, enter the following command:

'''python backup.py restore bucket-name::folderName local-directory'''
If the folder was not found in the s3 bucket, nothing will be backed up.
