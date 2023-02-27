import os
import sys
import boto3
import botocore

"""
AWS BACKUP AND RESTORATION PROGRAM
Amogh Kapalli

The following program backs up the files from a local directory to a specific s3 bucket and a folder in the bucket.
If the bucket or folder do not exist, the program creates a new bucket/folder with the specific contents of the local directory
The structure of the files and folders from the local directory will stay the same in the bucket

This program also restores files from a S3 bucket and a folder to a local directory given the path to that directory.
"""

# Evaluates the arguments given in the command line and retrieves user credentials for backup and restore executions
def main(argument):
    if len(argument) != 4:
        print("Incorrect number of arguments provided" + str(len(argument)))
        return
    try:
        s3_client = boto3.resource('s3')
        session = boto3.session.Session()
    except (botocore.exceptions.NoCredentialsError,
            botocore.exceptions.CredentialRetrievalError) as e:
        print("Problem retrieving credentials")
        exit()
    if argument[1] == "backup":
        localDirectory = argument[2]
        remote = argument[3]
        index = remote.find("::")
        if index == -1:
            print("incorrect remote directory or bucket name inputted")
            return
        remoteBucket = remote[:index]
        remoteDirectory = remote[index + 2:]
        print("backup: ")
        backup(s3_client, session, localDirectory, remoteBucket, remoteDirectory)
        print("backup from " + localDirectory + " to " + remoteBucket + " is completed")
    elif argument[1] == "restore":
        localDirectory = argument[3]
        remote = argument[2]
        index = remote.find("::")
        if (index == -1):
            print("incorrect remote directory or bucket name inputted")
            return
        remoteBucket = remote[:index]
        remoteDirectory = remote[index + 2:]
        print("restore: ")
        restore(s3_client, localDirectory, remoteBucket, remoteDirectory)
        print("Files from " + remoteBucket + " restored into " + localDirectory + " is completed")
    else:
        print("First argument must be backup or restore")
        exit()

#Backs up all the folders and files from the localDirectory into a S3 bucket and a remote directory in that bucket
#Creates a s3 bucket if the bucket is not found
def backup(s3_client, session, localDirectory, remoteBucket, remoteDirectory):
    bucket = s3_client.Bucket(remoteBucket)
    if bucket.creation_date is None:
        try:
            print("Bucket does not exist. Creating bucket: " + remoteBucket)
            s3_client.create_bucket(Bucket=remoteBucket,
                                    CreateBucketConfiguration={"LocationConstraint": session.region_name})
        except botocore.exceptions.ClientError as e:
            print(f"{str(e)}")
            return
        except botocore.exceptions.ParamValidationError as pe:
            print(f"Invalid bucket name inputted. Bucket name must match the regex ^[a-zA-Z0-9.\-_]{1, 255}$")
            return
    else:
        #Recursively traverses through the localDirectory using os.walk returning 3 values in each directory of the tree
        for root, dirs, files in os.walk(localDirectory):
            for file in files:
                local_path = os.path.join(root, file)
                # relative path to the file with respect to the local directory. its the path from root to the localDirectory
                relative_path = os.path.relpath(local_path, localDirectory)
                s3_path = os.path.join(remoteDirectory, relative_path).replace("\\", "/")

                current_s3_object = None

                try:
                    current_s3_object = s3_client.Object(remoteBucket, s3_path)
                except:
                    pass
                #checks if object was already in the bucket
                if current_s3_object is not None:
                    try:
                        s3_timestamp = current_s3_object.last_modified.timestamp()
                    # if object is not already in the bucket then this exception runs
                    except botocore.exceptions.ClientError as e:
                        print("Uploaded file: " + file + " successfully")
                        s3_client.Bucket(remoteBucket).upload_file(local_path, s3_path)
                        continue
                    directory_timestamp = os.path.getmtime(local_path)
                    if directory_timestamp <= s3_timestamp:
                        print(local_path + "already exists on S3 and hasn't been modified, skipping upload")
                        continue
                    else:
                        s3_client.Bucket(remoteBucket).upload_file(local_path, s3_path)
                        print("Uploaded file: " + file + " successfully")


#restore method restores the files from a s3 bucket into a localDirectory
def restore(s3_client, localDirectory, remoteBucket, remoteDirectory):

    s3_bucket = s3_client.Bucket(remoteBucket)

    if s3_bucket not in s3_client.buckets.all():
        print(remoteBucket + " does not exist. Cannot restore.")
        return False
    else:
        objects = s3_bucket.objects.filter(Prefix=remoteDirectory)

        # iterate overall objects in remote directory
        for obj in objects:
            if localDirectory is None:
                target = obj.key
            else:
                #full path of the objects on the local machine
                target = os.path.join(localDirectory, os.path.relpath(obj.key, remoteDirectory))

            #creates directory on local
            local_dir = os.path.dirname(target)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
                #skips directories
                if obj.key[-1] == '/':
                    continue
            # download from S3
            print(obj.key + " " + target)
            s3_bucket.download_file(obj.key, target)
            print("Restored file: " + obj.key + " successfully")


if __name__ == "__main__":
    main(sys.argv[0:])
