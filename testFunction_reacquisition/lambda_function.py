import json
import os
import boto3
import requests
import pymongo
import socket
import pytz
from datetime import timezone
from email.utils import parsedate_to_datetime
from pymongo.errors import ConnectionFailure
from email import policy
from email.parser import BytesParser
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
ec2 = boto3.client('ec2')

def get_external_ip():
    try:
        res = requests.get('https://api.ipify.org', timeout=50)
        return res.text
    except requests.RequestException:
        return "Failed to get external IP"

def update_security_group(external_ip):
    security_group_id = 'sg-03539a59e2530fb1d'

    if external_ip == "Failed to get external IP":
        print("Failed to get external IP. Cannot update security group.")
        return None

    try:
        response = ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 27017,
                    'ToPort': 27017,
                    'IpRanges': [{'CidrIp': f'{external_ip}/32'}]
                },
            ]
        )
        print(f'Security Group updated successfully.Add:{external_ip}')
        return response
    except Exception as e:
        print(f'Error updating security group: {str(e)}')
        return None

def remove_security_group_rule(external_ip):
    security_group_id = 'sg-03539a59e2530fb1d'

    try:
        response = ec2.revoke_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 27017,
                    'ToPort': 27017,
                    'IpRanges': [{'CidrIp': f'{external_ip}/32'}]
                },
            ]
        )
        print(f'Security Group rule removed successfully. Removed: {external_ip}')
        return response
    except Exception as e:
        print(f'Error removing security group rule: {str(e)}')
        return None

def move_file(source_bucket, source_key, destination_bucket, destination_key):
    try:
        # ファイルをコピー
        s3.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=destination_bucket,
            Key=destination_key
        )
        
        # 元のファイルを削除
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        
        print(f"Moved file from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
    except Exception as e:
        print(f"Error moving file {source_key}: {str(e)}")
        raise

def process_file(bucket_name, object_key, external_ip):
    try:
        # S3からファイルを取得
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        eml_content = response['Body'].read()

        # ファイルを解析
        msg = BytesParser(policy=policy.default).parsebytes(eml_content)

        # 受信日時を取得し、JSTに変換
        date_received = msg.get('Date')
        if date_received:
            date_utc = parsedate_to_datetime(date_received)
            jst = pytz.timezone('Asia/Tokyo')
            date_jst = date_utc.astimezone(jst)
            date_received = date_jst.isoformat()
        else:
            date_received = None

        # subjectを取得
        subject = msg.get('subject', 'No Subject')
        print('subject:', subject)

        email_subject = 'subject:' + subject
        body = msg.get_body(preferencelist=('plain', 'html'))
        if body:
            email_content = body.get_content() + email_subject + object_key
        else:
            email_content = f"No content available. {email_subject} {object_key}"
        print("content:", email_content)

        # Claudeにリクエスト
        prompt = (
            "投げた文章は5つのフィールドのみでMongoDBに取り込めるJson形式に成形してください。"
            "フィールド名はemail,info,date,subject,keyの5つです。"
            "subjectはsubject:の件名を加工せずに入れます。"
            "dateにはメールの受信日時がYYYY/MM/DD―hh:mmの形式で入ります。"
            "infoフィールドには改行を含めないでください。"
            "keyにはemails/job-offers/～のようなパスが入ります。"
            "案件の概要、スキル、金額、テレワークやリモートの有無、勤務時間や場所などを200文字以内にシンプルにまとめてください。"
        )

        headers = {
            'x-api-key': os.getenv("CLAUDE_API_KEY"),
            'Content-Type': 'application/json',
            'anthropic-version': '2023-06-01'
        }

        data = {
            'model': 'claude-3-haiku-20240307',
            'max_tokens': 1024,
            'system': prompt,
            "messages": [
                {"role": "user", "content": f"メール本文: {email_content}\n受信日時: {date_received}"}
            ]
        }

        response = requests.post('https://api.anthropic.com/v1/messages', headers=headers, json=data, timeout=99)
        response.raise_for_status()

        # 成形されたJSONを取得
        content = response.json()['content'][0]['text']
        print("content:", content)

        # JSONをパースし、infoフィールドの改行を除去
        document = json.loads(content)
        if 'info' in document:
            document['info'] = document['info'].replace('\n', ' ').replace('\r', '').strip()
            print("Processed content:", json.dumps(document, ensure_ascii=False))

        # MongoDBに接続してデータ書き込み
        mongo_uri = os.getenv('MONGODB_URI')
        client = pymongo.MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000
        )

        # 接続テスト
        client.admin.command('ismaster')

        db = client[os.getenv('MONGODB_DB_NAME')]
        collection = db[os.getenv('MONGODB_COLLECTION_NAME')]

        if date_received and 'date' not in document:
            document['date'] = date_received

        # 同じsubjectのレコードをチェック
        existing_document = collection.find_one({"subject": document['subject']})
        if existing_document:
            # 既存のレコードがある場合、古いレコードを削除
            print(f"同じsubjectのレコードが見つかりました。古いレコードを削除します: {document['subject']}")
            collection.delete_many({"subject": document['subject']})
            print("古いレコードを削除しました")

        # 新しいレコードを挿入
        collection.insert_one(document)
        print("新しいレコードを挿入しました")

        print("DB書き込み終了")

    except Exception as e:
        print(f"ファイル処理中にエラーが発生しました: {str(e)}")
        raise

    finally:
        if 'client' in locals():
            client.close()

def process_files(source_bucket, source_folder, function_name, continuation_token=None):
    kwargs = {
        'Bucket': source_bucket,
        'Prefix': source_folder if source_folder.endswith('/') else source_folder + '/'
    }
    if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

    response = s3.list_objects_v2(**kwargs)

    for item in response.get('Contents', []):
        source_key = item['Key']
        
        # フォルダ自体は処理しない
        if source_key.endswith('/'):
            continue
        
        print(f"Processing file: {source_key}")
        
        # ファイルを処理
        try:
            process_file(source_bucket, source_key, external_ip)
        except Exception as e:
            print(f"Error processing file {source_key}: {str(e)}")
            continue  # エラーが発生しても次のファイルの処理を続ける

    # まだ処理すべきファイルがある場合
    if response.get('IsTruncated', False):
        # 新しいLambda関数を呼び出して残りのファイルを処理
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps({
                'continuation_token': response['NextContinuationToken']
            })
        )

def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event, indent=2))

    global external_ip
    external_ip = None
    try:
        external_ip = get_external_ip()
        update_security_group(external_ip)

        source_bucket_name = os.environ['S3_BUCKET_NAME']
        source_folder_name = os.environ['S3_FOLDER_NAME']

        continuation_token = event.get('continuation_token')

        process_files(source_bucket_name, source_folder_name, 
                      context.function_name,
                      continuation_token)

        return {
            'statusCode': 200,
            'body': json.dumps('Processing and moving files completed successfully')
        }

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'サーバーエラー'})
        }

    finally:
        if external_ip:
            remove_security_group_rule(external_ip)