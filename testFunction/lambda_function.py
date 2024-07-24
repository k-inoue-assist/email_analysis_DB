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

# 外部IP取得
def get_external_ip():
    try:
        res = requests.get('https://api.ipify.org', timeout=50)
        return res.text
    except requests.RequestException:
        return "Failed to get external IP"

# セキュリティグループの追加
def update_security_group(external_ip):
    ec2 = boto3.client('ec2')
    security_group_id = 'sg-03539a59e2530fb1d'
    external_ip = get_external_ip()

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
        print('Security Group updated successfully')
        return response
    except Exception as e:
        print(f'Error updating security group: {str(e)}')
        return None

# 追加したセキュリティグループの削除
def remove_security_group_rule(external_ip):
    ec2 = boto3.client('ec2')
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
        print('Security Group rule removed successfully.remove:', external_ip)
        return response
    except Exception as e:
        print(f'Error removing security group rule: {str(e)}')
        return None

def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event, indent=2))

    try:
        host = socket.gethostname()
        ip = socket.gethostbyname(host)
        
        # イベントデータをログに出力
        print("Received event: " + json.dumps(event, indent=2))
        external_ip = get_external_ip()
        print("External IP:", external_ip)

        # S3イベントからバケット名とオブジェクトキーを取得
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        encoded_key = event['Records'][0]['s3']['object']['key']
        object_key = unquote_plus(encoded_key)

        print(f"Bucket name: {bucket_name}")
        print(f"Encoded object key: {encoded_key}")
        print(f"Decoded object key: {object_key}")

        # セキュリティグループを更新
        update_result = update_security_group(external_ip)
        if update_result is None:
            print("Failed to update security group")

        # バケットパスをチェック
        if not object_key.startswith('emails/job-offers/'):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid path'})
            }
        
        # S3からファイルを取得
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        eml_content = response['Body'].read()

        # ファイルを解析
        msg = BytesParser(policy=policy.default).parsebytes(eml_content)

        # 受信日時を取得し、JSTに変換
        date_received = msg['Date']
        if date_received:
            date_utc = parsedate_to_datetime(date_received)
            jst = pytz.timezone('Asia/Tokyo')
            date_jst = date_utc.astimezone(jst)
            date_received = date_jst.isoformat()
        else:
            date_received = None

        # subjectを取得
        subject = msg['subject']
        print ('subject:', subject)
        # subjectにSPAM又は人材情報が含まれているかチェック・含まれていればDB登録せずに処理を正常終了。
        spam_keywords = ['SPAM', 'スパムブロック', '要員ご紹介', '人材◇', '弊社個人', '技術者ご紹介', '弊社所属', 'Fabeee人材', '人材】', 'エムアイメイズ-人材', '要員のご紹介', '人材情報', '弊社社員', '注力人材', '要員情報', 'SI_人材', '弊社正社員', 'BTM人材', '人材紹介', '要員紹介', 'Re:', 'RE:', 'KANAME技術者', 'KANAME要員', 'KANAME社員', '技術者のご紹介', '直個人']
        if any(keyword in subject.upper() for keyword in spam_keywords):
            print("SPAM検出or人材紹介のためDB未登録: " + subject)
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'SPAM detected, skipped processing'})
            }

        email_subject = 'subject:' + subject
        email_content = msg.get_body(preferencelist=('plain', 'html')).get_content() + email_subject + object_key
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
            'system':prompt,
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
        try:
            document = json.loads(content)
            if 'info' in document:
               document['info'] = document['info'].replace('\n', ' ').replace('\r', '').strip()
               print("Processed content:", json.dumps(document, ensure_ascii=False))
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print("Original content:", content)
            raise

        # MongoDBに接続してデータ書き込み
        try:
            print("DB書き込み開始")
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

            document = json.loads(content)

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

            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Success'})
            }    
        except ConnectionFailure as e:
            print(f"MongoDBへの接続に失敗しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'データベース接続エラー'})
        }

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'サーバーエラー'})
        }
    finally:
        if 'client' in locals():
            client.close()
        if external_ip:
            remove_result = remove_security_group_rule(external_ip)
            if remove_result is None:
                print("Failed to remove security group rule")